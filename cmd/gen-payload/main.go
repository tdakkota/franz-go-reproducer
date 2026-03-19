// gen-payload writes a binary payload file that can be fed to producer -payload-file.
//
// Usage:
//
//	go run ./cmd/gen-payload -size 5MiB -pattern sequential -output payload.bin
package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
)

func main() {
	sizeStr := flag.String("size", "5MiB", "Payload size, human-readable (e.g. 1MiB, 512KB)")
	output := flag.String("output", "payload.bin", "Output file path")
	pattern := flag.String("pattern", "sequential", "Fill pattern: sequential | zeros | random")
	flag.Parse()

	size, err := humanize.ParseBytes(*sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid size %q: %v\n", *sizeStr, err)
		os.Exit(1)
	}

	buf := make([]byte, size)
	switch *pattern {
	case "sequential":
		for i := range buf {
			buf[i] = byte(i % 256)
		}
	case "zeros":
		// buf is already zeroed
	case "random":
		if _, err := rand.Read(buf); err != nil {
			fmt.Fprintf(os.Stderr, "rand.Read: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown pattern %q; choose: sequential, zeros, random\n", *pattern)
		os.Exit(1)
	}

	if err := os.WriteFile(*output, buf, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", *output, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s (%s, pattern=%s)\n", *output, humanize.IBytes(size), *pattern)
}
