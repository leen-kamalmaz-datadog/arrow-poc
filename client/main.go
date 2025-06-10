package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"

	pb "leen-grpc/icecreamservice"
)

func formatColumnName(s string) string {
	columnName := strings.ReplaceAll(s, "_", " ")
	caser := cases.Title(language.English)
	columnName = caser.String(columnName)
	return columnName
}

func processAndPrintArrowData(arrowData []byte) error {
	mem := memory.DefaultAllocator
	bufReader := bytes.NewReader(arrowData)

	// create an IPC reader (will read the schema first, then the record batches)
	arrowReader, err := ipc.NewReader(bufReader, ipc.WithAllocator(mem))
	if err != nil {
		return fmt.Errorf("Failed to create Arrow IPC reader: %w", err)
	}
	defer arrowReader.Release()

	fmt.Println("--- Arrow Data Received ---")
	recordCount := 0
	for arrowReader.Next() {
		record := arrowReader.Record()
		schema := arrowReader.Schema()

		recordCount++
		fmt.Printf("Processing Arrow RecordBatch %d with %d rows and %d columns.\n", recordCount, record.NumRows(), record.NumCols())

		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Println("------------------------------------")
			for c := 0; c < int(record.NumCols()); c++ {
				colName := formatColumnName(schema.Fields()[c].Name)
				if record.Column(c).IsNull(i) {
					fmt.Printf("%-20s%s\n", colName+":", "N/A")
				} else {
					fmt.Printf("%-20s%s\n", colName+":", record.Column(c).ValueStr(i))
				}
			}
		}
		record.Release() // release the current record batch
	}
	fmt.Println("------------------------------------")

	if arrowReader.Err() != nil && arrowReader.Err() != io.EOF {
		return fmt.Errorf("Error reading Arrow record batches: %w", err)
	}

	if recordCount == 0 {
		log.Println("No Arrow RecordBatches found in the data.")
	}

	return nil
}

func main() {
	fmt.Println("Ice Cream Service Client starting...")
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	// 1. Create an Arrow Flight client
	flightClient := flight.NewFlightServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	flavorsToQuery := []string{"Midnight Chocolate", "Strawberry Sorbet", "Non-Existent Flavor", "Pistachio Gelato", "Mystery Flavor"}

	// 2. Prepare the request message that will become the ticket content
	ticketRequest := &pb.FlavorDetailsRequest{
		Names: flavorsToQuery,
	}

	// 3. Marshal the request into bytes to create the ticket
	ticketBytes, err := proto.Marshal(ticketRequest)
	if err != nil {
		log.Fatalf("Failed to marshal ticket request: %v", err)
	}
	flightTicket := &flight.Ticket{Ticket: ticketBytes}

	log.Printf("Requesting details for flavors via Flight DoGet: %v", flavorsToQuery)

	// 4. Call the DoGet method with the ticket
	stream, err := flightClient.DoGet(ctx, flightTicket)
	if err != nil {
		log.Fatalf("Error calling Flight DoGet: %v", err)
	}

	// 5. Receive data from the DoGet stream
	// The server sends the entire IPC stream (schema + batches) in one FlightData message
	// Client collects all DataBody parts
	var allArrowDataBytes []byte
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			log.Println("DoGet stream finished.")
			break // End of stream
		}
		if err != nil {
			log.Fatalf("Error receiving data from DoGet stream: %v", err)
		}
		if data.DataBody != nil {
			allArrowDataBytes = append(allArrowDataBytes, data.DataBody...)
		}
		// Note: a Flight server could send schema in data.DataHeader and record batches in data.DataBody.
		// My server just writes the schema directly into the IPC stream buffer using ipc.WithSchema (so the schema is part of the DataBody content)
	}

	log.Printf("Response from DoGet:")

	if len(allArrowDataBytes) == 0 {
		log.Println("No Arrow data returned.")
		return
	}

	// process Arrow data using helper function
	if err := processAndPrintArrowData(allArrowDataBytes); err != nil {
		log.Fatalf("Failed to process Arrow data: %v", err)
	}
}
