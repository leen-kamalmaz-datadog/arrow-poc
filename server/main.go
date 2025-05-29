package main

import (
    "context"
    "database/sql" // standard SQL package
    "fmt"
    "log"
    "net"
    "os" // for getting environment variables
	"time" // for setting connection pool properties

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes" // for gRPC status codes
	"google.golang.org/grpc/status" // for gRPC status errors

	pb "leen-grpc/icecreamservice" // my generated protobuf package

    "github.com/lib/pq" // PostgreSQL driver, imported for side effects
)

type iceCreamServer struct {
	pb.UnimplementedIceCreamServiceServer
	db *sql.DB
}

func NewIceCreamServer(db *sql.DB) *iceCreamServer {
	return &iceCreamServer{db: db}
}

func (s *iceCreamServer) GetFlavorDetails(ctx context.Context, req *pb.GetFlavorDetailsRequest) (*pb.GetFlavorDetailsResponse, error) {
	flavorNames := req.GetNames()
	if len(flavorNames) == 0 {
		return &pb.GetFlavorDetailsResponse{Flavors: make(map[string]*pb.FlavorDetail)}, nil
	}

	log.Printf("Received GetFlavorDetails request for: %v", flavorNames)

	query := `
        SELECT
            name,
            COALESCE(description, '') AS description,
            base_type,
            includes_nuts,
            popularity_rating
            -- date_added removed from query
        FROM IceCreamFlavors
        WHERE name = ANY($1)`

	rows, err := s.db.QueryContext(ctx, query, pq.Array(flavorNames))
	if err != nil {
		log.Printf("Database query failed: %v", err)
		return nil, status.Errorf(codes.Internal, "database query failed: %v", err)
	}
	defer rows.Close()

	flavorsMap := make(map[string]*pb.FlavorDetail)
	for rows.Next() {
		var detail pb.FlavorDetail
		var sqlDescription sql.NullString
		var sqlRating sql.NullInt32 // to handle NULL rating

		err := rows.Scan(
			&detail.Name,
			&sqlDescription,
			&detail.BaseType,
			&detail.IncludesNuts,
			&sqlRating,
		)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			return nil, status.Errorf(codes.Internal, "failed to scan database row: %v", err)
		}
		
		if sqlDescription.Valid {
			detail.Description = sqlDescription.String
		} else {
			detail.Description = ""
		}

		// handling for optional popularity_rating
		if sqlRating.Valid {
			rating := sqlRating.Int32
			detail.PopularityRating = &rating
		}

		flavorsMap[detail.Name] = &detail
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error during rows iteration: %v", err)
		return nil, status.Errorf(codes.Internal, "database iteration error: %v", err)
	}

	log.Printf("Returning details for %d flavors.", len(flavorsMap))
	return &pb.GetFlavorDetailsResponse{Flavors: flavorsMap}, nil
}

func main() {
	fmt.Println("Ice Cream Service Server starting...")

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		databaseUrl = "postgres://leen:leens_password@localhost:5432/leensdatabase?sslmode=disable"
		log.Println("DATABASE_URL not set, using default.")
	}

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
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	iceCreamSvc := NewIceCreamServer(db)
	pb.RegisterIceCreamServiceServer(grpcServer, iceCreamSvc)

	log.Printf("gRPC server listening on %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
