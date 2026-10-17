package kafkaprobe

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

type fakeAdmin struct {
	start, end kadm.ListedOffsets
	committed  kadm.Offsets
	group      string
}

func (f *fakeAdmin) ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return f.start, nil
}

func (f *fakeAdmin) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return f.end, nil
}

func (f *fakeAdmin) CommitOffsets(_ context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error) {
	f.group, f.committed = group, os
	resp := kadm.OffsetResponses{}
	for topic, parts := range os {
		for p, o := range parts {
			if resp[topic] == nil {
				resp[topic] = map[int32]kadm.OffsetResponse{}
			}
			resp[topic][p] = kadm.OffsetResponse{Offset: o}
		}
	}
	return resp, nil
}

func listed(topic string, offsets ...int64) kadm.ListedOffsets {
	l := kadm.ListedOffsets{topic: map[int32]kadm.ListedOffset{}}
	for p, o := range offsets {
		l[topic][int32(p)] = kadm.ListedOffset{Topic: topic, Partition: int32(p), Offset: o}
	}
	return l
}

func TestSeekCommitsEndOrStart(t *testing.T) {
	f := &fakeAdmin{start: listed("t", 0, 0), end: listed("t", 10, 20)}

	var out bytes.Buffer
	require.NoError(t, Seek(t.Context(), f, SeekOptions{Topic: "t", Group: "g", To: "end"}, &out))
	require.Equal(t, "g", f.group)
	require.Equal(t, int64(20), f.committed["t"][1].At)
	require.Contains(t, out.String(), "partitions=2")

	require.NoError(t, Seek(t.Context(), f, SeekOptions{Topic: "t", Group: "g", To: "start"}, &out))
	require.Equal(t, int64(0), f.committed["t"][1].At)
}

func TestSeekFailsWithoutPartitions(t *testing.T) {
	f := &fakeAdmin{end: kadm.ListedOffsets{}}
	err := Seek(t.Context(), f, SeekOptions{Topic: "t", Group: "g", To: "end"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "no partitions")
}

func TestSeekOptionsValidation(t *testing.T) {
	require.Error(t, SeekOptions{Group: "g", To: "end"}.validate())
	require.Error(t, SeekOptions{Topic: "t", To: "end"}.validate())
	require.Error(t, SeekOptions{Topic: "t", Group: "g", To: "middle"}.validate())
}
