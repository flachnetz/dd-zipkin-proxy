package hyperjson

import (
	"github.com/pkg/errors"
	"io"
)

type Buffer struct {
	input io.Reader

	tail, head int
	eof        bool

	buffer []byte
}

func (b *Buffer) Current() byte {
	return b.buffer[b.tail]
}

func (b *Buffer) require(n int) error {
	for b.Available() < n {
		missing := n - b.Available()
		if b.free() < missing {
			return io.ErrShortBuffer
		}

		if err := b.LoadMore(); err != nil {
			return errors.WithMessagef(err, "require(%d)", n)
		}
	}

	return nil
}

// Number of bytes currently Available in the buffer
func (b *Buffer) Available() int {
	return b.head - b.tail
}

// Size of the free space in the buffer.
func (b *Buffer) free() int {
	return len(b.buffer) - b.Available()
}

// Load more data into the buffer.
func (b *Buffer) LoadMore() error {
	if b.eof {
		return io.EOF
	}

	if b.buffer == nil {
		b.buffer = make([]byte, 4096)
	}

	// move existing bytes to the beginning of the buffer
	if b.tail > 0 {
		previousCount := b.Available()
		copy(b.buffer[:previousCount], b.buffer[b.head:b.tail])
		b.tail = 0
		b.head = previousCount
	}

	// read more data into the rest
	count, err := b.input.Read(b.buffer[b.head:])
	if err == io.EOF {
		// stop in case of EOF
		b.eof = true
		if count == 0 {
			return io.EOF
		}

	} else if err != nil {
		return errors.WithMessage(err, "buffer more data")
	}

	b.head += count
	return nil
}
