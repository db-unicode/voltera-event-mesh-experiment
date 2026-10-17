package expctl

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlotCSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "s.csv")
	writeFile(t, csvPath, "ts,replicas,depth,note\n2026-10-16T10:00:00.000000000Z,0,5,a\n2026-10-16T10:00:02.000000000Z,2,,b\n2026-10-16T10:00:04.000000000Z,4,15,c\n")
	png := filepath.Join(dir, "fig.png")
	cols, err := PlotCSV(csvPath, "ts", nil, png, "t")
	require.NoError(t, err)
	require.Equal(t, []string{"replicas", "depth"}, cols, "note no es numérica")
	raw, err := os.ReadFile(png)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(raw, []byte("\x89PNG")))

	var out, errb bytes.Buffer
	svg := filepath.Join(dir, "fig.svg")
	require.Equal(t, 0, Run(context.Background(), []string{"plot", "--csv", csvPath, "--x", "ts", "--y", "replicas", "--out", svg}, &out, &errb, &fakeKube{}), errb.String())
	raw, err = os.ReadFile(svg)
	require.NoError(t, err)
	require.Contains(t, string(raw), "<svg")

	_, err = PlotCSV(csvPath, "ts", []string{"zz"}, png, "t")
	require.Error(t, err)
	_, err = PlotCSV(csvPath, "zz", nil, png, "t")
	require.Error(t, err)
}

func TestPlotNumericX(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "n.csv")
	writeFile(t, csvPath, "x,y\n1,2\n2,4\n")
	_, err := PlotCSV(csvPath, "x", []string{"y"}, filepath.Join(dir, "n.png"), "n")
	require.NoError(t, err)
}

func TestQuantileCmd(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "q.csv")
	writeFile(t, csvPath, "v\n1\n2\n3\n4\n5\n")
	var out, errb bytes.Buffer
	require.Equal(t, 0, Run(context.Background(), []string{"quantile", "--file", csvPath, "--col", "v", "--q", "0.5,0.95"}, &out, &errb, &fakeKube{}), errb.String())
	require.Contains(t, out.String(), `"0.5": 3`)
	require.Contains(t, out.String(), `"0.95": 4.8`)
	require.Equal(t, 1, Run(context.Background(), []string{"quantile", "--file", csvPath, "--col", "zz"}, &out, &errb, &fakeKube{}))
}

func writeRun(t *testing.T, results, exp, name, resultJSON, envJSON string) {
	t.Helper()
	run := filepath.Join(results, exp, name)
	writeFile(t, filepath.Join(run, "result.json"), resultJSON)
	if envJSON != "" {
		writeFile(t, filepath.Join(run, "env.json"), envJSON)
	}
	require.NoError(t, os.Symlink(name, filepath.Join(results, exp, "latest")))
}

func TestBuildReport(t *testing.T) {
	results := t.TempDir()
	writeRun(t, results, "e00-harness", "20261016T100000Z-abc1234-r1",
		`{"verdict":"PASS","criteria":[{"id":"c1","description":"d|x","metric":"m","observed":3,"op":"le","expected":5,"verdict":"PASS"}]}`,
		`{"git":{"sha":"abc1234","dirty":true},"startedAt":"2026-10-16T10:00:00Z"}`)
	writeRun(t, results, "e01", "20261016T110000Z-def5678-r2",
		`{"verdict":"FAIL","criteria":[{"id":"loss","metric":"m","observed":2,"op":"eq","expected":0,"verdict":"FAIL"},{"id":"gone","metric":"m","observed":null,"op":"ge","expected":1,"verdict":"FAIL","error":"no existe"}]}`, "")
	require.NoError(t, os.MkdirAll(filepath.Join(results, "final"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(results, "s00", "20261016T000000Z-x"), 0o755))

	md, err := BuildReport(results, filepath.Join(results, "summary.md"), time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Contains(t, md, "| e00-harness | PASS | 1/1 | 2026-10-16 10:00:00 UTC | `abc1234 (dirty)` | [20261016T100000Z-abc1234-r1](e00-harness/20261016T100000Z-abc1234-r1/) |")
	require.Contains(t, md, "| e01 | FAIL | 0/2 | 2026-10-16 11:00:00 UTC | `def5678` |")
	require.Contains(t, md, "| c1 | d\\|x | 3 | <= 5 | PASS |")
	require.Contains(t, md, "| gone |  | error: no existe | >= 1 | FAIL |")
	require.NotContains(t, md, "final")
	require.NotContains(t, md, "s00")
	require.Less(t, strings.Index(md, "e00-harness"), strings.Index(md, "e01"))

	var out, errb bytes.Buffer
	sum := filepath.Join(results, "summary.md")
	require.Equal(t, 0, Run(context.Background(), []string{"report", "--results", results, "--out", sum}, &out, &errb, &fakeKube{}), errb.String())
	_, err = os.Stat(sum)
	require.NoError(t, err)
}

func TestParseRunName(t *testing.T) {
	d, sha := parseRunName("20261016T100000Z-abc1234-dirty-r3")
	require.Equal(t, "2026-10-16 10:00:00 UTC", d)
	require.Equal(t, "abc1234-dirty", sha)
}
