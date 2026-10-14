// Command test_kv runs a few requests against a mokv node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/dynamic-calm/mokv/client"
)

func main() {
	addr := flag.String("addr", "localhost:8400", "service address")
	flag.Parse()

	c, err := client.New(*addr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	fmt.Println("Getting servers:")
	servers, err := c.Servers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range servers {
		fmt.Printf("\t- %v -> is leader: %v\n", s.RPCAddr, s.IsLeader)
	}

	fmt.Println("\nSetting key 'hello' = 'world'")
	if err := c.Set(ctx, "hello", []byte("world"), time.Minute); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Getting key 'hello'")
	value, err := c.Get(ctx, "hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got: hello = %s\n", value)
}
