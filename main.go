package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mateopresacastro/mokv/run"
)

func main() {
	ctx := context.Background()
	if err := run.Run(ctx, os.Getenv, run.Config{}); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
