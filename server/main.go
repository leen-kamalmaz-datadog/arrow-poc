package main

import (
    "bytes"
    "context"
    "database/sql"					// standard SQL package
    "fmt"
    "log"
    "net"
	"time"							// for setting connection pool properties

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"	// for gRPC status codes
	"google.golang.org/grpc/status"	// for gRPC status errors

    "github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"

	pb "leen-grpc/icecreamservice"	// my generated protobuf package

    "github.com/lib/pq"				// PostgreSQL driver
)

type iceCreamServer struct {
	pb.UnimplementedIceCreamServiceServer
	db *sql.DB
}

func NewIceCreamServer(db *sql.DB) *iceCreamServer {
	return &iceCreamServer{db: db}
}

func (s *iceCreamServer) GetFlavorDetailsArrow(ctx context.Context, req *pb.GetFlavorDetailsRequest) (*pb.GetFlavorDetailsArrowResponse, error) {
	flavorNames := req.GetNames()
	if len(flavorNames) == 0 { // return an empty successful response.
		return &pb.GetFlavorDetailsArrowResponse{ArrowData: []byte{}}, nil
	}

	log.Printf("Received GetFlavorDetailsArrow request for: %v", flavorNames)

	// 1. Define the Arrow schema based on query results (this can be more dynamic in the future)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},				// name: TEXT -> string
			{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},			// description: TEXT -> string
			{Name: "base_type", Type: arrow.BinaryTypes.String, Nullable: false},			// base_type: TEXT -> string
			{Name: "includes_nuts", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},	// includes_nuts: BOOLEAN -> bool
			{Name: "popularity_rating", Type: arrow.PrimitiveTypes.Int32, Nullable: true},	// popularity_rating: INT (nullable) -> int32
		},
		nil, // no schema metadata
	)

	// 2. Initialize Arrow memory allocator and builders
	mem := memory.DefaultAllocator

	builders := make([]array.Builder, len(schema.Fields())) // each element in this slice will be a builder for a specific column
	defer func() { // ensure all builders are released before retuning (to prevent memory leaks)
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
			return nil, status.Errorf(codes.Internal, "unsupported Arrow type for builder: %s", field.Type.Name())
		}
	}

	// 3. Fetch data from PostgreSQL
	query := `
        SELECT
            name,
            description,
            base_type,
            includes_nuts,
            popularity_rating
        FROM IceCreamFlavors
        WHERE name = ANY($1)`

	rows, err := s.db.QueryContext(ctx, query, pq.Array(flavorNames)) // executes the SQL query against the database
	if err != nil {
		log.Printf("Database query failed: %v", err)
		return nil, status.Errorf(codes.Internal, "database query failed: %v", err)
	}
	defer rows.Close()

	// 4. Iterate over rows and populate Arrow builders

	scanDest := make([]interface{}, len(schema.Fields())) // to hold pointers to rows.Scan
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
				var ni sql.NullInt32 // use sql.NullInt32 for nullable integers
				valueHolders[i] = &ni // store the address of a NullInt32
			} else {
				var i32 int32
				valueHolders[i] = &i32 // store the address of a int32
			}
		default:
			return nil, status.Errorf(codes.Internal, "unsupported Arrow type in schema for scan argument setup: %s", field.Type.Name())
		}
		scanDest[i] = valueHolders[i] // scanDest holds pointers to the variables in valueHolders
	}

	rowCount := 0
	for rows.Next() {
		err := rows.Scan(scanDest...) // scan the column values from the database into the memory locations pointed to by the elements in scanDest

		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			return nil, status.Errorf(codes.Internal, "failed to scan database row: %v", err)
		}

		// dynamically append the scanned values to the correct builders
		for i, field := range schema.Fields() {
			builder := builders[i] // get the Arrow builder for the column
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
				return nil, status.Errorf(codes.Internal, "unsupported Arrow type for appending data: %s for field %s", field.Type.Name(), field.Name)
			}
		}
		rowCount++
	}

	if err := rows.Err(); err != nil { // checking if an error occurred while fetching rows
		log.Printf("Error during rows iteration: %v", err)
		return nil, status.Errorf(codes.Internal, "database iteration error: %v", err)
	}

	// 5. Create Arrow arrays from builders
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

	// 6. Create an Arrow record (a single batch of data)
	record := array.NewRecord(schema, arrowArrays, int64(rowCount))
	defer record.Release()

	// 7. Serialize the Arrow record to IPC stream format in a buffer
	var buf bytes.Buffer
	// ipc.NewWriter writes data in the Arrow IPC _stream_ format
	// 		this format includes the schema first, then the record batches
	ipcWriter := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema())) // can also use ipc.WithAllocator(mem)
	
	err = ipcWriter.Write(record)
	if err != nil {
		log.Printf("Failed to write Arrow record to buffer: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to write Arrow record: %v", err)
	}

	err = ipcWriter.Close() // Close is important to finalize the stream (write end-of-stream marker)
	if err != nil {
		log.Printf("Failed to close Arrow IPC writer: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to close Arrow writer: %v", err)
	}

	log.Printf("Successfully converted %d rows to Arrow format.", rowCount)

	// 8. Return the Arrow data in the response
	return &pb.GetFlavorDetailsArrowResponse{ArrowData: buf.Bytes()}, nil
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
	iceCreamSvc := NewIceCreamServer(db)
	pb.RegisterIceCreamServiceServer(grpcServer, iceCreamSvc)

	log.Printf("gRPC server listening on %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
