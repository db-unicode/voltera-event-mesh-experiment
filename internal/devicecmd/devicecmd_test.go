package devicecmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignVerify(t *testing.T) {
	key := []byte("secret")
	body, sig, err := Encode(key, Command{MatchID: "m1", Seller: "s", Buyer: "b", KWh: 2, IssuedAt: time.Unix(0, 0).UTC()})
	require.NoError(t, err)
	require.JSONEq(t, `{"matchId":"m1","seller":"s","buyer":"b","kwh":2,"issuedAt":"1970-01-01T00:00:00Z"}`, string(body))
	require.True(t, Verify(key, body, sig))
	require.False(t, Verify([]byte("other"), body, sig))
	require.False(t, Verify(key, append(body, ' '), sig))
	require.False(t, Verify(key, body, "not-hex"))
	require.False(t, Verify(key, body, ""))
	// vector público de HMAC-SHA256 (el spike S15 firma igual con openssl)
	require.Equal(t, "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
		Sign([]byte("key"), []byte("The quick brown fox jumps over the lazy dog")))
}

func TestValidate(t *testing.T) {
	require.Error(t, Command{Seller: "s", Buyer: "b", KWh: 1}.Validate())
	require.Error(t, Command{MatchID: "m", Buyer: "b", KWh: 1}.Validate())
	require.Error(t, Command{MatchID: "m", Seller: "s", Buyer: "b"}.Validate())
	_, _, err := Encode([]byte("k"), Command{})
	require.Error(t, err)
}
