package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/dynamic-calm/mokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	addr := flag.String("addr", "localhost:9400", "service address")
	flag.Parse()

	cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer cc.Close()

	client := api.NewKVClient(cc)
	ctx := context.Background()

	// Get Servers
	fmt.Println("Getting servers:")
	servers, err := client.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range servers.Servers {
		fmt.Printf("\t- %v -> is leader: %v\n", s, s.IsLeader)
	}

	// Set
	fmt.Println("Setting key 'hello' = 'world'")
	setRes, err := client.Set(ctx, &api.SetRequest{Key: "hello", Value: []byte("world")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Set OK: %v\n\n", setRes.Ok)

	// Get
	fmt.Println("Getting key 'hello'")
	getRes, err := client.Get(ctx, &api.GetRequest{Key: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got: %s = %s\n\n", getRes.Key, string(getRes.Value))

}
