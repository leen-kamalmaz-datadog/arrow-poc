package main

import (
    "context"
	"time"
    "fmt"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
	pb "leen-grpc/icecreamservice"
)

func main() {
	fmt.Println("Ice Cream Service Client starting...")
	conn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewIceCreamServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	flavorsToQuery := []string{"Midnight Chocolate", "Strawberry Sorbet", "NonExistentFlavor", "Pistachio Gelato", "Mystery Flavor"}
	request := &pb.GetFlavorDetailsRequest{
		Names: flavorsToQuery,
	}

	log.Printf("Requesting details for flavors: %v", flavorsToQuery)
	response, err := client.GetFlavorDetails(ctx, request)
	if err != nil {
		log.Fatalf("Error calling GetFlavorDetails: %v", err)
	}

	log.Printf("Response from GetFlavorDetails:")
	if response.GetFlavors() == nil || len(response.GetFlavors()) == 0 {
		log.Println("No flavor details returned.")
	} else {
		for _, detail := range response.GetFlavors() {
			fmt.Println("------------------------------------")
			log.Printf("Flavor:          %s", detail.GetName())
			log.Printf("Description:     %s", detail.GetDescription())
			log.Printf("Base Type:       %s", detail.GetBaseType())
			log.Printf("Includes Nuts:   %t", detail.GetIncludesNuts())

			if detail.PopularityRating != nil {
				log.Printf("Popularity:      %d/5 stars", detail.GetPopularityRating())
			} else {
				log.Println("Popularity:      N/A")
			}
		}
		fmt.Println("------------------------------------")
	}
}