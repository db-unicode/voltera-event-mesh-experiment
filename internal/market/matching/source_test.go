package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/events"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/testutil"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/view"
)

func TestViewSourceReadsPositionsAndPrice(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	st := view.New(rdb, nil)
	ctx := context.Background()
	until := time.Now().Add(time.Hour).UTC()
	untilStr := until.Format(time.RFC3339Nano)

	put := func(key string, ver int64, fields map[string]any) {
		_, err := st.Upsert(ctx, prefix+key, ver, fields, time.Minute)
		require.NoError(t, err)
	}
	put("pos:z-1:m1", 4, map[string]any{"meterId": "m1", "side": "SURPLUS", "kwh": "2.5", "validUntil": untilStr, "zone": "z-1"})
	put("pos:z-2:m2", 1, map[string]any{"meterId": "m2", "side": "DEFICIT", "kwh": "1", "validUntil": untilStr, "zone": "z-2"})
	put("pos:z-1:bad", 1, map[string]any{"meterId": "bad", "side": "X", "kwh": "1", "validUntil": untilStr, "zone": "z-1"})
	put("price:r1", 9, map[string]any{"pricePerKwh": "0.21", "validUntil": untilStr})

	src := ViewSource{Store: st, KeyPrefix: prefix}
	byZone, invalid, err := src.Positions(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, invalid)
	require.Equal(t, map[string][]Position{
		"z-1": {{MeterID: "m1", Side: events.SideSurplus, KWh: 2.5, ValidUntil: until, Zone: "z-1", Version: 4}},
		"z-2": {{MeterID: "m2", Side: events.SideDeficit, KWh: 1, ValidUntil: until, Zone: "z-2", Version: 1}},
	}, byZone)

	price, err := src.Price(ctx, "r1")
	require.NoError(t, err)
	require.Equal(t, Price{PricePerKWh: 0.21, ValidUntil: until, Version: 9}, price)

	_, err = src.Price(ctx, "r9")
	require.ErrorIs(t, err, view.ErrNotFound)
}
