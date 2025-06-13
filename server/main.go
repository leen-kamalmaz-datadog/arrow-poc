package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/lib/pq"

	pb "leen-grpc/icecreamservice"
)

var pgToArrowTypeMap = map[string]arrow.DataType{
	"TEXT":    arrow.BinaryTypes.String,
	"VARCHAR": arrow.BinaryTypes.String,
	"BOOL":    arrow.FixedWidthTypes.Boolean,
	"INT4":    arrow.PrimitiveTypes.Int32,
}

type iceCreamServer struct {
	flight.BaseFlightServer // embed BaseFlightServer for default implementations
	// pb.UnimplementedIceCreamServiceServer
	db *sql.DB
}

func NewIceCreamServer(db *sql.DB) *iceCreamServer {
	return &iceCreamServer{db: db}
}

func (s *iceCreamServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// 1. Deserialize the ticket to get flavor names
	// reuse existing FlavorDetailsRequest protobuf message for the ticket content
	req := &pb.FlavorDetailsRequest{}
	if err := proto.Unmarshal(ticket.GetTicket(), req); err != nil {
		return status.Errorf(codes.InvalidArgument, "DoGet: failed to unmarshal ticket: %v", err)
	}
	flavorNames := req.GetNames()

	if len(flavorNames) == 0 { // return an empty successful stream
		return nil
	}

	log.Printf("DoGet: Received request for flavors: %v", flavorNames)

	// 2. Fetch data from PostgreSQL
	query := `
        SELECT
            name,
            description,
            base_type,
            includes_nuts,
            popularity_rating
        FROM IceCreamFlavors
        WHERE name = ANY($1)`

	rows, err := s.db.QueryContext(stream.Context(), query, pq.Array(flavorNames)) // executes the SQL query against the database (uses context from stream)
	if err != nil {
		log.Printf("DoGet: Database query failed: %v", err)
		return status.Errorf(codes.Internal, "DoGet: database query failed: %v", err)
	}
	defer rows.Close()

	// 3. Define the Arrow schema dynamically based on query results

	// get the column type metadata from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Printf("DoGet: Failed to get column types: %v", err)
		return status.Errorf(codes.Internal, "DoGet: failed to get column types: %v", err)
	}

	fields := make([]arrow.Field, len(columnTypes))
	for i, col := range columnTypes {
		// determine the Arrow data type from the database type name
		arrowType, ok := pgToArrowTypeMap[col.DatabaseTypeName()]
		log.Printf("DatabaseTypeName type: %v", col.DatabaseTypeName())
		log.Printf("arrow type: %v", arrowType)
		if !ok {
			return status.Errorf(codes.Internal, "DoGet: %v", fmt.Errorf("unsupported database type: %s", col.DatabaseTypeName()))
		}

		// create the Arrow Field for this column
		fields[i] = arrow.Field{
			Name: col.Name(),
			Type: arrowType,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// 4. Initialize Arrow memory allocator and builders
	mem := memory.DefaultAllocator
	recordBuilder := array.NewRecordBuilder(mem, schema)
	defer recordBuilder.Release()

	// 5. Iterate over rows and populate Arrow builders
	scanDest := make([]interface{}, len(schema.Fields()))     // to hold pointers to rows.Scan
	valueHolders := make([]interface{}, len(schema.Fields())) // to hold pointers to the correctly typed Go variables that will receive the data for each column
	for i, field := range schema.Fields() {
		// create a holder variable of the Go types for scanning, based on the Arrow field types
		switch field.Type.ID() {
		case arrow.STRING, arrow.BINARY, arrow.LARGE_STRING, arrow.LARGE_BINARY:
			var s string
			valueHolders[i] = &s // store the address of a string
		case arrow.BOOL:
			var b bool
			valueHolders[i] = &b // store the address of a bool
		case arrow.INT32:
			var ni sql.NullInt32  // use sql.NullInt32 for nullable integers
			valueHolders[i] = &ni // store the address of a NullInt32
		default:
			return status.Errorf(codes.Internal, "DoGet: unsupported Arrow type in schema for scan argument setup: %s", field.Type.Name())
		}
		scanDest[i] = valueHolders[i] // scanDest holds pointers to the variables in valueHolders
	}

	rowCount := 0
	for rows.Next() {
		err := rows.Scan(scanDest...) // scan the column values from the database into the memory locations pointed to by the elements in scanDest

		if err != nil {
			log.Printf("DoGet: Failed to scan row: %v", err)
			return status.Errorf(codes.Internal, "DoGet: failed to scan database row: %v", err)
		}

		// dynamically append the scanned values to the correct builders
		for i, field := range schema.Fields() {
			builder := recordBuilder.Fields()[i] // get the Arrow builder for the column
			valuePtr := valueHolders[i]          // get the pointer to the scanned value

			switch field.Type.ID() { // type-assert the generic builder and append the scanned data to the correct builder
			case arrow.STRING, arrow.BINARY, arrow.LARGE_STRING, arrow.LARGE_BINARY:
				val := *(valuePtr.(*string)) // dereference the pointer to get the value
				builder.(*array.StringBuilder).Append(val)
			case arrow.BOOL:
				val := *(valuePtr.(*bool))
				builder.(*array.BooleanBuilder).Append(val)
			case arrow.INT32:
				b := builder.(*array.Int32Builder)
				sqlVal := *(valuePtr.(*sql.NullInt32))
				if sqlVal.Valid {
					b.Append(sqlVal.Int32)
				} else {
					b.AppendNull()
				}
			default:
				return status.Errorf(codes.Internal, "DoGet: unsupported Arrow type for appending data: %s for field %s", field.Type.Name(), field.Name)
			}
		}
		rowCount++
	}

	if err := rows.Err(); err != nil { // checking if an error occurred while fetching rows
		log.Printf("DoGet: Error during rows iteration: %v", err)
		return status.Errorf(codes.Internal, "DoGet: database iteration error: %v", err)
	}

	// 6. Create Arrow arrays from builders
	arrowArrays := make([]arrow.Array, len(recordBuilder.Fields()))
	defer func() { // ensure all arrays are released
		for _, arr := range arrowArrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()
	for i, b := range recordBuilder.Fields() {
		arrowArrays[i] = b.NewArray()
	}

	// 7. Create an Arrow record (a single batch of data)
	record := array.NewRecord(schema, arrowArrays, int64(rowCount))
	defer record.Release()

	// 8. Send data using Flight Writer
	var buf bytes.Buffer
	// ipc.NewWriter writes data in the Arrow IPC _stream_ format
	// 		this format includes the schema first, then the record batches
	ipcWriter := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema())) // can also use ipc.WithAllocator(mem)

	if err := ipcWriter.Write(record); err != nil { // translates record into the Arrow IPC byte sequence, writing the bytes into the buf
		log.Printf("DoGet: Failed to write record to flight stream: %v", err)
		return status.Errorf(codes.Internal, "DoGet: failed to write flight record: %v", err)
	}

	if err := ipcWriter.Close(); err != nil {
		log.Printf("DoGet: Failed to close IPC writer: %v", err)
		return status.Errorf(codes.Internal, "DoGet: failed to close IPC writer: %v", err)
	}

	err = stream.Send(&flight.FlightData{
		DataBody: buf.Bytes(),
	})
	if err != nil {
		log.Printf("DoGet: Failed to send flight data: %v", err)
		return status.Errorf(codes.Internal, "DoGet: failed to send flight data: %v", err)
	}

	log.Printf("DoGet: Successfully converted and sent %d rows in Arrow format.", rowCount)
	return nil // indicate successful completion of the stream
}

func main() {
	fmt.Println("Ice Cream Service Server starting...")

	databaseUrl := "postgres://leen:leens_password@localhost:5432/leensdatabase?sslmode=disable"

	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		log.Fatalf("Unable to open database connection: %v\n", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()
	err = db.PingContext(ctxPing)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	iceCreamFlightSvc := NewIceCreamServer(db)
	flight.RegisterFlightServiceServer(grpcServer, iceCreamFlightSvc)

	log.Printf("gRPC server (with Flight) listening on %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
