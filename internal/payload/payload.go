package payload

import (
	"crypto/rand"
	"fmt"
	"os"
)

// Generate fills a buffer of the given size using the named pattern
// ("sequential", "zeros", "random"). Returns an error for unknown patterns.
func Generate(size uint64, pattern string) ([]byte, error) {
	buf := make([]byte, size)
	switch pattern {
	case "sequential":
		for i := range buf {
			buf[i] = byte(i % 256)
		}
	case "zeros":
		// buf is already zeroed
	case "random":
		if _, err := rand.Read(buf); err != nil {
			return nil, fmt.Errorf("rand.Read: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown pattern %q; choose: sequential, zeros, random", pattern)
	}
	return buf, nil
}

// Load reads path if non-empty, otherwise calls Generate(size, "sequential").
func Load(path string, size uint64) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	return Generate(size, "sequential")
}
