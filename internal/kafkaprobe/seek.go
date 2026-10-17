package kafkaprobe

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kadm"
)

// SeekOptions configura el subcomando seek: fija los offsets confirmados de un grupo.
type SeekOptions struct {
	Topic string
	Group string
	// To es "end" o "start".
	To string
}

func (o SeekOptions) validate() error {
	switch {
	case o.Topic == "":
		return errors.New("--topic is required")
	case o.Group == "":
		return errors.New("--group is required")
	case o.To != "end" && o.To != "start":
		return fmt.Errorf("--to must be 'end' or 'start', got %q", o.To)
	}
	return nil
}

// Admin es la parte de *kadm.Client que usa Seek.
type Admin interface {
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
}

// Seek confirma para el grupo el inicio o el final actual de cada partición.
// Crea el grupo si no existe, de modo que el exportador pueda medir su lag desde 0.
// El grupo no debe tener miembros activos.
func Seek(ctx context.Context, a Admin, opts SeekOptions, out io.Writer) error {
	if err := opts.validate(); err != nil {
		return err
	}
	list := a.ListEndOffsets
	if opts.To == "start" {
		list = a.ListStartOffsets
	}
	listed, err := list(ctx, opts.Topic)
	if err != nil {
		return fmt.Errorf("list offsets: %w", err)
	}
	if err := listed.Error(); err != nil {
		return fmt.Errorf("list offsets: %w", err)
	}
	offsets := listed.Offsets()
	if len(offsets[opts.Topic]) == 0 {
		return fmt.Errorf("topic %q has no partitions", opts.Topic)
	}
	resp, err := a.CommitOffsets(ctx, opts.Group, offsets)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	_, err = fmt.Fprintf(out, "seek group=%s topic=%s to=%s partitions=%d\n",
		opts.Group, opts.Topic, opts.To, len(offsets[opts.Topic]))
	return err
}
