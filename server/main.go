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

	// 1. Define the Arrow Schema based on query results
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
	mem := memory.DefaultAllocator // or memory.NewGoAllocator()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release() // ensure memory is released, regardless of how the function exits
	descriptionBuilder := array.NewStringBuilder(mem)
	defer descriptionBuilder.Release()
	baseTypeBuilder := array.NewStringBuilder(mem)
	defer baseTypeBuilder.Release()
	includesNutsBuilder := array.NewBooleanBuilder(mem)
	defer includesNutsBuilder.Release()
	popularityRatingBuilder := array.NewInt32Builder(mem)
	defer popularityRatingBuilder.Release()

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

	rows, err := s.db.QueryContext(ctx, query, pq.Array(flavorNames))
	if err != nil {
		log.Printf("Database query failed: %v", err)
		return nil, status.Errorf(codes.Internal, "database query failed: %v", err)
	}
	defer rows.Close()

	// 4. Iterate over rows and populate Arrow builders
	rowCount := 0
	for rows.Next() {
		var nameVal, descriptionVal, baseTypeVal string
		var includesNutsVal bool
		var popularityRatingVal sql.NullInt32 // use sql.NullInt32 for nullable integers

		err := rows.Scan(
			&nameVal,
			&descriptionVal,
			&baseTypeVal,
			&includesNutsVal,
			&popularityRatingVal,
		)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			return nil, status.Errorf(codes.Internal, "failed to scan database row: %v", err)
		}

		nameBuilder.Append(nameVal)
		descriptionBuilder.Append(descriptionVal)
		baseTypeBuilder.Append(baseTypeVal)
		includesNutsBuilder.Append(includesNutsVal)
		if popularityRatingVal.Valid {
			popularityRatingBuilder.Append(popularityRatingVal.Int32)
		} else {
			popularityRatingBuilder.AppendNull()
		}
		rowCount++
	}

	if err := rows.Err(); err != nil { // checking if an error occurred while fetching rows
		log.Printf("Error during rows iteration: %v", err)
		return nil, status.Errorf(codes.Internal, "database iteration error: %v", err)
	}

	// 5. Create Arrow Arrays from builders
	nameArray := nameBuilder.NewStringArray()
	defer nameArray.Release()
	descriptionArray := descriptionBuilder.NewStringArray()
	defer descriptionArray.Release()
	baseTypeArray := baseTypeBuilder.NewStringArray()
	defer baseTypeArray.Release()
	includesNutsArray := includesNutsBuilder.NewBooleanArray()
	defer includesNutsArray.Release()
	popularityRatingArray := popularityRatingBuilder.NewInt32Array()
	defer popularityRatingArray.Release()

	// 6. Create an Arrow Record (a single batch of data)
	record := array.NewRecord( // if rowCount is 0, NewRecord will still work and create a record with 0 rows
		schema,
		[]arrow.Array{
			nameArray,
			descriptionArray,
			baseTypeArray,
			includesNutsArray,
			popularityRatingArray,
		},
		int64(rowCount),
	)
	defer record.Release()

	// 7. Serialize the Arrow Record to IPC stream format in a buffer
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
