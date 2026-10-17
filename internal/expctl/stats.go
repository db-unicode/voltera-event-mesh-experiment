package expctl

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Percentile calcula el cuantil q (0..1) con interpolación lineal sobre vals (cualquier orden).
func Percentile(vals []float64, q float64) (float64, error) {
	if len(vals) == 0 {
		return 0, fmt.Errorf("sin datos")
	}
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("cuantil fuera de [0,1]: %v", q)
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	pos := q * float64(len(s)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	return s[lo] + (s[hi]-s[lo])*(pos-float64(lo)), nil
}

var pctFunc = regexp.MustCompile(`^p(\d+(?:\.\d+)?)$`)

// ApplyFunc aplica max, min, last, first, count, sum, avg o pNN sobre vals.
func ApplyFunc(name string, vals []float64) (float64, error) {
	if name == "count" {
		return float64(len(vals)), nil
	}
	if len(vals) == 0 {
		return 0, fmt.Errorf("%s: sin datos", name)
	}
	switch name {
	case "max":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Max(m, v)
		}
		return m, nil
	case "min":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Min(m, v)
		}
		return m, nil
	case "first":
		return vals[0], nil
	case "last":
		return vals[len(vals)-1], nil
	case "sum", "avg":
		var s float64
		for _, v := range vals {
			s += v
		}
		if name == "avg" {
			s /= float64(len(vals))
		}
		return s, nil
	}
	if m := pctFunc.FindStringSubmatch(name); m != nil {
		p, _ := strconv.ParseFloat(m[1], 64)
		return Percentile(vals, p/100)
	}
	return 0, fmt.Errorf("función desconocida %q", name)
}

// Table es un CSV en memoria con cabecera.
type Table struct {
	Header []string
	Rows   [][]string
}

// ReadCSV lee un CSV con cabecera.
func ReadCSV(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	recs, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(recs) == 0 {
		return nil, fmt.Errorf("%s: CSV vacío", path)
	}
	return &Table{Header: recs[0], Rows: recs[1:]}, nil
}

// Index devuelve el índice de la columna o -1.
func (t *Table) Index(col string) int {
	for i, h := range t.Header {
		if h == col {
			return i
		}
	}
	return -1
}

// Strings devuelve las celdas no vacías de la columna.
func (t *Table) Strings(col string) ([]string, error) {
	i := t.Index(col)
	if i < 0 {
		return nil, fmt.Errorf("columna %q no existe (columnas: %s)", col, strings.Join(t.Header, ","))
	}
	var out []string
	for _, row := range t.Rows {
		if i < len(row) && strings.TrimSpace(row[i]) != "" {
			out = append(out, row[i])
		}
	}
	return out, nil
}

// Floats devuelve los valores numéricos de la columna; ignora celdas vacías y falla con no numéricas.
func (t *Table) Floats(col string) ([]float64, error) {
	cells, err := t.Strings(col)
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(cells))
	for _, c := range cells {
		v, err := strconv.ParseFloat(strings.TrimSpace(c), 64)
		if err != nil {
			return nil, fmt.Errorf("columna %q: valor no numérico %q", col, c)
		}
		out = append(out, v)
	}
	return out, nil
}
