package expctl

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func TestJSONPath(t *testing.T) {
	var doc any
	require.NoError(t, json.Unmarshal([]byte(`{"a":{"b":[10,20,{"c":"x"}]},"k.y":1}`), &doc))
	cases := map[string]any{
		"$.a.b[1]":      float64(20),
		"$.a.b[-1].c":   "x",
		"$['k.y']":      float64(1),
		"$.a.b[2]['c']": "x",
	}
	for path, want := range cases {
		got, err := JSONPath(doc, path)
		require.NoError(t, err, path)
		require.Equal(t, want, got, path)
	}
	for _, bad := range []string{"a.b", "$.zz", "$.a.b[9]", "$.a.b.c", "$.a[0]", "$.a.b[x]", "$..a"} {
		_, err := JSONPath(doc, bad)
		require.Error(t, err, bad)
	}
}

func TestEvalMetric(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "metrics.json"), `{"lost":0,"nested":{"ok":true,"name":"e0"},"series":[1,5,3]}`)
	writeFile(t, filepath.Join(dir, "raw", "samples.csv"), "ts,replicas,depth\n1,0,\n2,3,10\n3,5,20\n4,2,30\n")

	cases := map[string]any{
		"metrics.json:$.lost":               float64(0),
		"metrics.json:$.nested.ok":          true,
		"metrics.json:$.nested.name":        "e0",
		"metrics.json:max($.series)":        float64(5),
		"metrics.json:count($.series)":      float64(3),
		"raw/samples.csv:max(replicas)":     float64(5),
		"raw/samples.csv:min(replicas)":     float64(0),
		"raw/samples.csv:last(replicas)":    float64(2),
		"raw/samples.csv:first(replicas)":   float64(0),
		"raw/samples.csv:count(depth)":      float64(3),
		"raw/samples.csv:p50(depth)":        float64(20),
		"raw/samples.csv:p95(depth)":        float64(29),
		"raw/samples.csv:p99(replicas)":     4.94,
		"raw/samples.csv:count(replicas)":   float64(4),
		"raw/samples.csv: max( replicas  )": float64(5),
	}
	for metric, want := range cases {
		got, err := EvalMetric(dir, metric)
		require.NoError(t, err, metric)
		if f, ok := want.(float64); ok {
			require.InDelta(t, f, got, 1e-9, metric)
		} else {
			require.Equal(t, want, got, metric)
		}
	}
	for _, bad := range []string{"metrics.json", "nofile.json:$.a", "raw/samples.csv:replicas", "raw/samples.csv:max(zz)", "metrics.json:$.zz", "x.txt:a", "metrics.json:max($.lost)"} {
		_, err := EvalMetric(dir, bad)
		require.Error(t, err, bad)
	}
}

func TestCompare(t *testing.T) {
	ok, err := Compare(0.0, "le", 1)
	require.NoError(t, err)
	require.True(t, ok)
	for _, c := range []struct {
		o        any
		op       string
		e        any
		expected bool
	}{
		{5.0, "lt", 5, false}, {5.0, "ge", 5, true}, {5.0, "gt", 5, false}, {5.0, "eq", 5, true},
		{true, "eq", true, true}, {"a", "eq", "a", true}, {"a", "eq", "b", false}, {0.1 + 0.2, "eq", 0.3, true},
	} {
		got, err := Compare(c.o, c.op, c.e)
		require.NoError(t, err)
		require.Equal(t, c.expected, got, "%v %s %v", c.o, c.op, c.e)
	}
	_, err = Compare("a", "le", "b")
	require.Error(t, err)
	_, err = Compare(1.0, "ne", 1)
	require.Error(t, err)
}

func TestLoadCriteria(t *testing.T) {
	dir := t.TempDir()
	list := filepath.Join(dir, "list.yaml")
	writeFile(t, list, "- {id: a, metric: 'm.json:$.x', op: le, value: 1}\n- id: b\n  description: d\n  metric: 'c.csv:max(v)'\n  op: ge\n  value: ${THRESHOLD}\n")
	t.Setenv("THRESHOLD", "46")
	cs, err := LoadCriteria(list)
	require.NoError(t, err)
	require.Len(t, cs, 2)
	require.Equal(t, 46, cs[1].Value)

	wrapped := filepath.Join(dir, "wrapped.yaml")
	writeFile(t, wrapped, "criteria:\n  - {id: a, metric: 'm.json:$.x', op: eq, value: 0}\n")
	cs, err = LoadCriteria(wrapped)
	require.NoError(t, err)
	require.Len(t, cs, 1)

	for name, body := range map[string]string{
		"noid.yaml":   "- {metric: m.json:$.x, op: le, value: 1}\n",
		"badop.yaml":  "- {id: a, metric: m.json:$.x, op: ne, value: 1}\n",
		"dup.yaml":    "- {id: a, metric: m.json:$.x, op: le, value: 1}\n- {id: a, metric: m.json:$.x, op: le, value: 1}\n",
		"novalue":     "- {id: a, metric: m.json:$.x, op: le}\n",
		"missing.yml": "- {id: a, metric: m.json:$.x, op: le, value: ${NO_SUCH_VAR_XYZ}}\n",
		"empty.yaml":  "[]\n",
	} {
		p := filepath.Join(dir, name)
		writeFile(t, p, body)
		_, err := LoadCriteria(p)
		require.Error(t, err, name)
	}
}

func TestRunAssertExitCodes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "metrics.json"), `{"lost":0}`)
	writeFile(t, filepath.Join(dir, "s.csv"), "ts,r\n1,1\n2,4\n")
	pass := filepath.Join(dir, "pass.yaml")
	fail := filepath.Join(dir, "fail.yaml")
	writeFile(t, pass, "- {id: lost, description: sin pérdida, metric: 'metrics.json:$.lost', op: eq, value: 0}\n- {id: peak, metric: 's.csv:max(r)', op: le, value: 4}\n")
	writeFile(t, fail, "- {id: lost, metric: 'metrics.json:$.lost', op: eq, value: 0}\n- {id: peak, metric: 's.csv:max(r)', op: lt, value: 4}\n- {id: gone, metric: 'nope.json:$.x', op: eq, value: 1}\n")

	var out, errb bytes.Buffer
	res := filepath.Join(dir, "result.json")
	require.Equal(t, 0, Run(context.Background(), []string{"assert", "--criteria", pass, "--data", dir, "--out", res}, &out, &errb, &fakeKube{}), errb.String())
	var ar AssertResult
	raw, err := os.ReadFile(res)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &ar))
	require.Equal(t, "PASS", ar.Verdict)
	require.Len(t, ar.Criteria, 2)
	require.Equal(t, "lost", ar.Criteria[0].ID)
	require.Equal(t, "eq", ar.Criteria[0].Op)

	out.Reset()
	require.Equal(t, 1, Run(context.Background(), []string{"assert", "--criteria", fail, "--data", dir, "--out", res}, &out, &errb, &fakeKube{}))
	raw, err = os.ReadFile(res)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &ar))
	require.Equal(t, "FAIL", ar.Verdict)
	require.Equal(t, []string{"PASS", "FAIL", "FAIL"}, []string{ar.Criteria[0].Verdict, ar.Criteria[1].Verdict, ar.Criteria[2].Verdict})
	require.NotEmpty(t, ar.Criteria[2].Error)
	require.Contains(t, out.String(), "FAIL  peak")

	require.Equal(t, 2, Run(context.Background(), []string{"assert"}, &out, &errb, &fakeKube{}))
}
