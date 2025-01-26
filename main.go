package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mateopresacastro/kv/kvd"
)

func main() {
	ctx := context.Background()
	if err := kvd.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
