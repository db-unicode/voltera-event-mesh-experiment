package expctl

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCmdHTTPGetAndPost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"method":"` + r.Method + `","path":"` + r.URL.RequestURI() + `"}`))
	}))
	defer srv.Close()
	kube := &fakeKube{forwardTo: srv}
	var out, errb bytes.Buffer

	require.Equal(t, 0, Run(context.Background(), []string{"http-get", "ns/svc:9090/state"}, &out, &errb, kube), errb.String())
	require.JSONEq(t, `{"method":"GET","path":"/state"}`, out.String())

	out.Reset()
	args := []string{"http-get", "--method", "POST", "ns/pod/p1:9090/admin/failpoint?name=a&mode=off"}
	require.Equal(t, 0, Run(context.Background(), args, &out, &errb, kube), errb.String())
	require.JSONEq(t, `{"method":"POST","path":"/admin/failpoint?name=a&mode=off"}`, out.String())

	require.Equal(t, 1, Run(context.Background(), []string{"http-get", "ns/svc:9090/boom"}, &out, &errb, kube))
	require.Contains(t, errb.String(), "HTTP 500")
	require.Equal(t, 2, Run(context.Background(), []string{"http-get", "mal"}, &out, &errb, kube))
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func sqlKube(psqlOut string) *fakeKube {
	return &fakeKube{handler: func(args []string) ([]byte, error) {
		if hasArgs(args, "get", "secret", "positions-db-credentials") {
			return []byte(`{"data":{"POSTGRES_USER":"` + b64("voltera") + `","POSTGRES_PASSWORD":"` + b64("pw") + `","POSTGRES_DB":"` + b64("voltera") + `"}}`), nil
		}
		return []byte(psqlOut), nil
	}}
}

func TestCmdSQL(t *testing.T) {
	kube := sqlKube("n,name\n3,abc\n")
	var out, errb bytes.Buffer
	require.Equal(t, 0, Run(context.Background(), []string{"sql", "voltera-r1/positions-db", "select count(*) as n"}, &out, &errb, kube), errb.String())
	require.Equal(t, "n,name\n3,abc\n", out.String())

	last := kube.calls[len(kube.calls)-1]
	require.True(t, hasArgs(last, "exec", "statefulset/positions-db", "-c", "postgres"))
	require.True(t, hasArgs(last, "PGPASSWORD=pw", "psql", "-X"))
	require.Equal(t, "select count(*) as n", last[len(last)-1])
	require.True(t, hasArgs(last, "--csv"))

	out.Reset()
	require.Equal(t, 0, Run(context.Background(), []string{"sql", "voltera-r1/positions-db", "q", "--json"}, &out, &errb, kube), errb.String())
	require.JSONEq(t, `[{"n":3,"name":"abc"}]`, out.String())

	require.Equal(t, 2, Run(context.Background(), []string{"sql", "voltera-r1/positions-db", "q", "--json", "--no-header"}, &out, &errb, kube))
	require.Equal(t, 2, Run(context.Background(), []string{"sql", "solo"}, &out, &errb, kube))
}
