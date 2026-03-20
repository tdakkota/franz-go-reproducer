package flagutil

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

// BytesFlag is a flag.Value that accepts human-readable byte sizes (e.g. "50MiB", "10MB").
type BytesFlag uint64

func (b *BytesFlag) String() string { return humanize.IBytes(uint64(*b)) }
func (b *BytesFlag) Set(s string) error {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	*b = BytesFlag(v)
	return nil
}
