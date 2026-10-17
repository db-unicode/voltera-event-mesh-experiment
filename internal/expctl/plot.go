package expctl

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
)

const (
	defaultDPI    = 96
	defaultWidth  = 8.0 // pulgadas
	defaultHeight = 4.0
)

// PlotOpts ajusta el tamaño de salida. El cero equivale a 8x4 pulgadas a 96 dpi (PNG 768x384).
// Las fuentes están en puntos, así que escalan solas con el dpi.
type PlotOpts struct {
	DPI    int
	Width  float64 // pulgadas
	Height float64
}

// parseSize interpreta "WxH" en pulgadas.
func parseSize(s string) (w, h float64, err error) {
	ws, hs, ok := strings.Cut(strings.ToLower(strings.TrimSpace(s)), "x")
	if ok {
		if w, err = strconv.ParseFloat(ws, 64); err == nil {
			h, err = strconv.ParseFloat(hs, 64)
		}
	}
	if !ok || err != nil || w <= 0 || h <= 0 {
		return 0, 0, fmt.Errorf("--size %q inválido, se espera WxH en pulgadas (p. ej. 9x4.5)", s)
	}
	return w, h, nil
}

// savePlot guarda p; los PNG con dpi distinto de 96 se renderizan con vgimg.
func savePlot(p *plot.Plot, o PlotOpts, out string) error {
	w, h := o.Width, o.Height
	if w <= 0 || h <= 0 {
		w, h = defaultWidth, defaultHeight
	}
	vw, vh := vg.Length(w)*vg.Inch, vg.Length(h)*vg.Inch
	if o.DPI <= 0 || o.DPI == defaultDPI || !strings.EqualFold(filepath.Ext(out), ".png") {
		return p.Save(vw, vh, out)
	}
	c := vgimg.NewWith(vgimg.UseWH(vw, vh), vgimg.UseDPI(o.DPI))
	p.Draw(draw.New(c))
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	if _, err := (vgimg.PngCanvas{Canvas: c}).WriteTo(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// xValues convierte la columna X: timestamps RFC3339 pasan a segundos desde el primero;
// si no, deben ser números. Devuelve también la etiqueta del eje.
func xValues(t *Table, col string) ([]float64, string, error) {
	i := t.Index(col)
	if i < 0 {
		return nil, "", fmt.Errorf("columna X %q no existe", col)
	}
	xs := make([]float64, len(t.Rows))
	var t0 time.Time
	isTime := false
	for r, row := range t.Rows {
		if i >= len(row) {
			return nil, "", fmt.Errorf("fila %d sin columna %q", r+2, col)
		}
		if ts, err := time.Parse(time.RFC3339Nano, row[i]); err == nil {
			if r == 0 {
				t0, isTime = ts, true
			}
			xs[r] = ts.Sub(t0).Seconds()
			continue
		}
		if isTime {
			return nil, "", fmt.Errorf("fila %d: %q no es un timestamp", r+2, row[i])
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(row[i]), 64)
		if err != nil {
			return nil, "", fmt.Errorf("fila %d: %q no es número ni timestamp", r+2, row[i])
		}
		xs[r] = f
	}
	if isTime {
		return xs, "t (s)", nil
	}
	return xs, col, nil
}

// numericColumns devuelve las columnas (salvo x) con al menos un valor y todos numéricos.
func numericColumns(t *Table, x string) []string {
	var cols []string
	for _, h := range t.Header {
		if h == x {
			continue
		}
		vals, err := t.Floats(h)
		if err == nil && len(vals) > 0 {
			cols = append(cols, h)
		}
	}
	return cols
}

// PlotCSV dibuja las columnas ys (o todas las numéricas si ys está vacío) contra x y guarda out
// (formato por extensión: png, svg, pdf). Devuelve las columnas dibujadas.
func PlotCSV(csvPath, x string, ys []string, out, title string) ([]string, error) {
	return PlotCSVOpts(csvPath, x, ys, out, title, PlotOpts{})
}

// PlotCSVOpts es PlotCSV con tamaño y dpi configurables.
func PlotCSVOpts(csvPath, x string, ys []string, out, title string, o PlotOpts) ([]string, error) {
	t, err := ReadCSV(csvPath)
	if err != nil {
		return nil, err
	}
	xs, xlabel, err := xValues(t, x)
	if err != nil {
		return nil, err
	}
	if len(ys) == 0 {
		ys = numericColumns(t, x)
	}
	if len(ys) == 0 {
		return nil, fmt.Errorf("%s: sin columnas numéricas que dibujar", csvPath)
	}
	p := plot.New()
	p.Title.Text = title
	p.X.Label.Text = xlabel
	p.Legend.Top = true
	for ci, col := range ys {
		idx := t.Index(col)
		if idx < 0 {
			return nil, fmt.Errorf("columna %q no existe en %s", col, csvPath)
		}
		var pts plotter.XYs
		for r, row := range t.Rows {
			if idx >= len(row) || strings.TrimSpace(row[idx]) == "" {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(row[idx]), 64)
			if err != nil {
				return nil, fmt.Errorf("columna %q: valor no numérico %q", col, row[idx])
			}
			pts = append(pts, plotter.XY{X: xs[r], Y: v})
		}
		if len(pts) == 0 {
			continue
		}
		line, err := plotter.NewLine(pts)
		if err != nil {
			return nil, err
		}
		line.Color = plotutil.Color(ci)
		p.Add(line)
		p.Legend.Add(col, line)
	}
	if err := savePlot(p, o, out); err != nil {
		return nil, err
	}
	return ys, nil
}

func cmdPlot(args []string, stderr io.Writer) error {
	fs := newFlagSet("plot", stderr)
	csvPath := fs.String("csv", "", "CSV de entrada")
	x := fs.String("x", "ts", "columna X (timestamps RFC3339 o números)")
	y := fs.String("y", "", "columnas Y separadas por coma (por defecto todas las numéricas)")
	out := fs.String("out", "", "imagen de salida (.png, .svg)")
	title := fs.String("title", "", "título (por defecto el nombre del CSV)")
	dpi := fs.Int("dpi", defaultDPI, "resolución del PNG (la imagen mide tamaño*dpi píxeles)")
	size := fs.String("size", "8x4", "tamaño WxH en pulgadas")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *csvPath == "" || *out == "" {
		return usageErr("uso: plot --csv x.csv --x ts --y col1,col2 --out fig.png [--dpi 96] [--size 8x4]")
	}
	if *dpi <= 0 {
		return usageErr("--dpi debe ser > 0")
	}
	w, h, err := parseSize(*size)
	if err != nil {
		return usageErr("%s", err)
	}
	var ys []string
	for _, c := range strings.Split(*y, ",") {
		if c = strings.TrimSpace(c); c != "" {
			ys = append(ys, c)
		}
	}
	if *title == "" {
		*title = *csvPath
	}
	cols, err := PlotCSVOpts(*csvPath, *x, ys, *out, *title, PlotOpts{DPI: *dpi, Width: w, Height: h})
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stderr, "plot: %s (%s)\n", *out, strings.Join(cols, ","))
	return nil
}
