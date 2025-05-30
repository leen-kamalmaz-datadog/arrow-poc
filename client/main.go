package main

import (
    "context"
	"time"
    "fmt"
	"io"
    "log"
	"bytes"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

	"github.com/apache/arrow/go/v16/arrow/array"	// For specific array types
	"github.com/apache/arrow/go/v16/arrow/ipc"		// For IPC reader
	"github.com/apache/arrow/go/v16/arrow/memory"	// For memory allocator
	
	pb "leen-grpc/icecreamservice"
)

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
		recordCount++
		log.Printf("Processing Arrow RecordBatch %d with %d rows and %d columns.", recordCount, record.NumRows(), record.NumCols())

		// expecting schema: name (string), description (string), base_type (string), includes_nuts (bool), popularity_rating (int32, nullable)
		// can also get the schema dynamically: schema := record.Schema()

		nameCol := record.Column(0).(*array.String)
		descriptionCol := record.Column(1).(*array.String)
		baseTypeCol := record.Column(2).(*array.String)
		includesNutsCol := record.Column(3).(*array.Boolean)
		popularityRatingCol := record.Column(4).(*array.Int32) // Nullable

		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Println("------------------------------------")
			log.Printf("Flavor:          %s", nameCol.Value(i))
			log.Printf("Description:     %s", descriptionCol.Value(i))
			log.Printf("Base Type:       %s", baseTypeCol.Value(i))
			log.Printf("Includes Nuts:   %t", includesNutsCol.Value(i))

			if popularityRatingCol.IsNull(i) {
				log.Println("Popularity:      N/A")
			} else {
				log.Printf("Popularity:      %d/5 stars", popularityRatingCol.Value(i))
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
	conn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewIceCreamServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	flavorsToQuery := []string{"Midnight Chocolate", "Strawberry Sorbet", "Non-Existent Flavor", "Pistachio Gelato", "Mystery Flavor"}
	request := &pb.GetFlavorDetailsRequest{
		Names: flavorsToQuery,
	}

	log.Printf("Requesting Arrow details for flavors: %v", flavorsToQuery)
	arrowResponse, err := client.GetFlavorDetailsArrow(ctx, request)
	if err != nil {
		log.Fatalf("Error calling GetFlavorDetailsArrow: %v", err)
	}

	log.Printf("Response from GetFlavorDetailsArrow:")
	arrowData := arrowResponse.GetArrowData()

	if len(arrowData) == 0 {
		log.Println("No Arrow data returned.")
		return
	}

	// process Arrow data using helper function
	if err := processAndPrintArrowData(arrowData); err != nil {
		log.Fatalf("Failed to process Arrow data: %v", err)
	}
}