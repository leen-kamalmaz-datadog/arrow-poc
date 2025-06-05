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

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/lib/pq"

	pb "leen-grpc/icecreamservice"
)

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
	// reuse existing GetFlavorDetailsRequest protobuf message for the ticket content
	req := &pb.GetFlavorDetailsRequest{}
	if err := proto.Unmarshal(ticket.GetTicket(), req); err != nil {
		return status.Errorf(codes.InvalidArgument, "DoGet: failed to unmarshal ticket: %v", err)
	}
	flavorNames := req.GetNames()

	if len(flavorNames) == 0 { // return an empty successful stream
		return nil
	}

	log.Printf("DoGet: Received request for flavors: %v", flavorNames)

	// 2. Define the Arrow schema based on query results (this can be more dynamic in the future)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},               // name: TEXT -> string
			{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},        // description: TEXT -> string
			{Name: "base_type", Type: arrow.BinaryTypes.String, Nullable: false},          // base_type: TEXT -> string
			{Name: "includes_nuts", Type: arrow.FixedWidthTypes.Boolean, Nullable: false}, // includes_nuts: BOOLEAN -> bool
			{Name: "popularity_rating", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, // popularity_rating: INT (nullable) -> int32
		},
		nil, // no schema metadata
	)

	// 3. Initialize Arrow memory allocator and builders
	mem := memory.DefaultAllocator

	builders := make([]array.Builder, len(schema.Fields())) // each element in this slice will be a builder for a specific column
	defer func() {                                          // ensure all builders are released before retuning (to prevent memory leaks)
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	// create builders dynamically based on the schema
	for i, field := range schema.Fields() {
		switch field.Type.ID() { // type identifier
		case arrow.STRING, arrow.BINARY, arrow.LARGE_STRING, arrow.LARGE_BINARY:
			builders[i] = array.NewStringBuilder(mem)
		case arrow.BOOL:
			builders[i] = array.NewBooleanBuilder(mem)
		case arrow.INT32:
			builders[i] = array.NewInt32Builder(mem)
		// there are more types that can be added, but not needed for this specific POC
		default:
			return status.Errorf(codes.Internal, "DoGet: unsupported Arrow type for builder: %s", field.Type.Name())
		}
	}

	// 4. Fetch data from PostgreSQL
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
			if field.Nullable { // if the DB column (and therefore Arrow field) can be NULL
				var ni sql.NullInt32  // use sql.NullInt32 for nullable integers
				valueHolders[i] = &ni // store the address of a NullInt32
			} else {
				var i32 int32
				valueHolders[i] = &i32 // store the address of an int32
			}
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
			builder := builders[i]      // get the Arrow builder for the column
			valuePtr := valueHolders[i] // get the pointer to the scanned value

			switch field.Type.ID() { // type-assert the generic builder and append the scanned data to the correct builder
			case arrow.STRING, arrow.BINARY, arrow.LARGE_STRING, arrow.LARGE_BINARY:
				val := *(valuePtr.(*string)) // dereference the pointer to get the value
				builder.(*array.StringBuilder).Append(val)
			case arrow.BOOL:
				val := *(valuePtr.(*bool))
				builder.(*array.BooleanBuilder).Append(val)
			case arrow.INT32:
				b := builder.(*array.Int32Builder)
				if field.Nullable { // if the Arrow field can be NULL
					sqlVal := *(valuePtr.(*sql.NullInt32))
					if sqlVal.Valid {
						b.Append(sqlVal.Int32)
					} else {
						b.AppendNull()
					}
				} else {
					val := *(valuePtr.(*int32))
					b.Append(val)
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
	arrowArrays := make([]arrow.Array, len(builders))
	defer func() { // ensure all arrays are released
		for _, arr := range arrowArrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()
	for i, b := range builders {
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
