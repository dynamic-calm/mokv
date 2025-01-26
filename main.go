package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mateopresacastro/mokv/kv"
)

func main() {
	ctx := context.Background()
	if err := kv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
