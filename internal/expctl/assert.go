package expctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Criterion es un criterio declarativo de criteria.yaml.
type Criterion struct {
	ID          string `yaml:"id"`
	Description string `yaml:"description"`
	Metric      string `yaml:"metric"`
	Op          string `yaml:"op"`
	Value       any    `yaml:"value"`
}

// CriterionResult es el veredicto de un criterio.
type CriterionResult struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Metric      string `json:"metric"`
	Observed    any    `json:"observed"`
	Op          string `json:"op"`
	Expected    any    `json:"expected"`
	Verdict     string `json:"verdict"`
	Error       string `json:"error,omitempty"`
}

// AssertResult es el contenido de result.json.
type AssertResult struct {
	Verdict  string            `json:"verdict"`
	Data     string            `json:"data"`
	Criteria []CriterionResult `json:"criteria"`
}

var envVar = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// LoadCriteria lee criteria.yaml: una lista de criterios o un mapa con la clave `criteria`.
// Expande ${VAR} con el entorno (falla si falta alguna variable).
func LoadCriteria(path string) ([]Criterion, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var missing []string
	expanded := envVar.ReplaceAllStringFunc(string(raw), func(m string) string {
		name := envVar.FindStringSubmatch(m)[1]
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
		}
		return v
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: variables sin definir: %s", path, strings.Join(missing, ", "))
	}
	var list []Criterion
	if err := yaml.Unmarshal([]byte(expanded), &list); err != nil {
		var wrapped struct {
			Criteria []Criterion `yaml:"criteria"`
		}
		if err2 := yaml.Unmarshal([]byte(expanded), &wrapped); err2 != nil {
			return nil, fmt.Errorf("%s: %w", path, err2)
		}
		list = wrapped.Criteria
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("%s: sin criterios", path)
	}
	seen := map[string]bool{}
	for i, c := range list {
		switch {
		case c.ID == "":
			return nil, fmt.Errorf("%s: criterio #%d sin id", path, i+1)
		case seen[c.ID]:
			return nil, fmt.Errorf("%s: id duplicado %q", path, c.ID)
		case c.Metric == "":
			return nil, fmt.Errorf("%s: criterio %s sin metric", path, c.ID)
		case c.Value == nil:
			return nil, fmt.Errorf("%s: criterio %s sin value", path, c.ID)
		}
		if !validOp(c.Op) {
			return nil, fmt.Errorf("%s: criterio %s: op %q inválido (le|lt|ge|gt|eq)", path, c.ID, c.Op)
		}
		seen[c.ID] = true
	}
	return list, nil
}

func validOp(op string) bool {
	switch op {
	case "le", "lt", "ge", "gt", "eq":
		return true
	}
	return false
}

// numeric convierte v a float64 (bool = 0/1).
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// Compare evalúa observed <op> expected.
func Compare(observed any, op string, expected any) (bool, error) {
	if !validOp(op) {
		return false, fmt.Errorf("op %q inválido", op)
	}
	o, ook := numeric(observed)
	e, eok := numeric(expected)
	if ook && eok {
		switch op {
		case "le":
			return o <= e, nil
		case "lt":
			return o < e, nil
		case "ge":
			return o >= e, nil
		case "gt":
			return o > e, nil
		default:
			return math.Abs(o-e) <= 1e-9*math.Max(1, math.Abs(e)), nil
		}
	}
	if op != "eq" {
		return false, fmt.Errorf("op %s requiere números (observed=%v, expected=%v)", op, observed, expected)
	}
	return fmt.Sprint(observed) == fmt.Sprint(expected), nil
}

var funcCall = regexp.MustCompile(`^([a-z][a-z0-9.]*)\((.*)\)$`)

// EvalMetric evalúa "archivo:expresión" dentro de dataDir.
// Archivos .json: JSONPath simple ($.a.b[0]); .csv: func(columna).
// func(...) también aplica a un array numérico en JSON, p. ej. max($.series).
func EvalMetric(dataDir, metric string) (any, error) {
	file, expr, ok := strings.Cut(metric, ":")
	if !ok || file == "" || expr == "" {
		return nil, fmt.Errorf("metric %q: formato archivo:expresión", metric)
	}
	path := filepath.Join(dataDir, file)
	expr = strings.TrimSpace(expr)
	switch strings.ToLower(filepath.Ext(file)) {
	case ".csv":
		m := funcCall.FindStringSubmatch(expr)
		if m == nil {
			return nil, fmt.Errorf("metric %q: para CSV usa func(columna), p. ej. max(replicas)", metric)
		}
		t, err := ReadCSV(path)
		if err != nil {
			return nil, err
		}
		var vals []float64
		if m[1] == "count" {
			cells, err := t.Strings(strings.TrimSpace(m[2]))
			if err != nil {
				return nil, err
			}
			return float64(len(cells)), nil
		}
		if vals, err = t.Floats(strings.TrimSpace(m[2])); err != nil {
			return nil, err
		}
		return ApplyFunc(m[1], vals)
	case ".json":
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var doc any
		if err := dec.Decode(&doc); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if m := funcCall.FindStringSubmatch(expr); m != nil {
			v, err := JSONPath(doc, strings.TrimSpace(m[2]))
			if err != nil {
				return nil, err
			}
			arr, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("metric %q: %s no es un array", metric, m[2])
			}
			if m[1] == "count" {
				return float64(len(arr)), nil
			}
			vals := make([]float64, 0, len(arr))
			for _, a := range arr {
				f, ok := numeric(a)
				if !ok {
					return nil, fmt.Errorf("metric %q: elemento no numérico %v", metric, a)
				}
				vals = append(vals, f)
			}
			return ApplyFunc(m[1], vals)
		}
		v, err := JSONPath(doc, expr)
		if err != nil {
			return nil, err
		}
		if n, ok := v.(json.Number); ok {
			return n.Float64()
		}
		return v, nil
	}
	return nil, fmt.Errorf("metric %q: extensión no soportada (usa .json o .csv)", metric)
}

// JSONPath evalúa un subconjunto: $, .clave, ['clave'], [n] y [-n].
func JSONPath(doc any, path string) (any, error) {
	if !strings.HasPrefix(path, "$") {
		return nil, fmt.Errorf("jsonpath %q: debe empezar por $", path)
	}
	cur := doc
	rest := path[1:]
	for rest != "" {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			end := strings.IndexAny(rest, ".[")
			if end < 0 {
				end = len(rest)
			}
			key := rest[:end]
			rest = rest[end:]
			obj, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("jsonpath %q: %q no es un objeto", path, key)
			}
			if cur, ok = obj[key]; !ok {
				return nil, fmt.Errorf("jsonpath %q: clave %q no existe", path, key)
			}
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, fmt.Errorf("jsonpath %q: falta ]", path)
			}
			inner := rest[1:end]
			rest = rest[end+1:]
			if len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0] {
				key := inner[1 : len(inner)-1]
				obj, ok := cur.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("jsonpath %q: %q no es un objeto", path, key)
				}
				if cur, ok = obj[key]; !ok {
					return nil, fmt.Errorf("jsonpath %q: clave %q no existe", path, key)
				}
				continue
			}
			idx, err := strconv.Atoi(inner)
			if err != nil {
				return nil, fmt.Errorf("jsonpath %q: índice inválido %q", path, inner)
			}
			arr, ok := cur.([]any)
			if !ok {
				return nil, fmt.Errorf("jsonpath %q: no es un array", path)
			}
			if idx < 0 {
				idx += len(arr)
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("jsonpath %q: índice %s fuera de rango (len %d)", path, inner, len(arr))
			}
			cur = arr[idx]
		default:
			return nil, fmt.Errorf("jsonpath %q: sintaxis no soportada en %q", path, rest)
		}
	}
	return cur, nil
}

// Evaluate evalúa todos los criterios contra dataDir.
func Evaluate(dataDir string, criteria []Criterion) AssertResult {
	res := AssertResult{Verdict: "PASS", Data: dataDir}
	for _, c := range criteria {
		cr := CriterionResult{ID: c.ID, Description: c.Description, Metric: c.Metric, Op: c.Op, Expected: c.Value, Verdict: "FAIL"}
		obs, err := EvalMetric(dataDir, c.Metric)
		if err != nil {
			cr.Error = err.Error()
		} else {
			cr.Observed = obs
			ok, err := Compare(obs, c.Op, c.Value)
			if err != nil {
				cr.Error = err.Error()
			} else if ok {
				cr.Verdict = "PASS"
			}
		}
		if cr.Verdict != "PASS" {
			res.Verdict = "FAIL"
		}
		res.Criteria = append(res.Criteria, cr)
	}
	return res
}

var opSymbol = map[string]string{"le": "<=", "lt": "<", "ge": ">=", "gt": ">", "eq": "=="}

func cmdAssert(args []string, stdout, stderr io.Writer) (failed bool, err error) {
	fs := newFlagSet("assert", stderr)
	criteria := fs.String("criteria", "", "criteria.yaml")
	data := fs.String("data", "", "carpeta de la corrida")
	out := fs.String("out", "", "result.json de salida")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return false, err
	}
	if len(pos) != 0 || *criteria == "" || *data == "" {
		return false, usageErr("uso: assert --criteria criteria.yaml --data <dir> --out result.json")
	}
	cs, err := LoadCriteria(*criteria)
	if err != nil {
		return false, err
	}
	res := Evaluate(*data, cs)
	for _, c := range res.Criteria {
		line := fmt.Sprintf("%s  %s: observado=%v %s esperado=%v", c.Verdict, c.ID, c.Observed, opSymbol[c.Op], c.Expected)
		if c.Error != "" {
			line += "  (" + c.Error + ")"
		}
		_, _ = fmt.Fprintln(stdout, line)
	}
	_, _ = fmt.Fprintln(stdout, res.Verdict)
	if *out != "" {
		var buf bytes.Buffer
		if err := writeJSON(&buf, res); err != nil {
			return false, err
		}
		if err := os.WriteFile(*out, buf.Bytes(), 0o644); err != nil {
			return false, err
		}
	}
	return res.Verdict != "PASS", nil
}
