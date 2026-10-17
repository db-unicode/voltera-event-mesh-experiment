// Package expctl implementa el arnés de experimentos: consultas a Prometheus, muestreo,
// trazado de eventos por logs, criterios declarativos, gráficas y reporte.
package expctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const usageText = `expctl: arnés de experimentos (corre en el host; usa kubectl --context $KUBE_CONTEXT, por defecto kind-voltera)

Subcomandos:
  now                                           hora actual RFC3339Nano UTC (9 decimales)
  elapsed <start> [end]                         segundos entre dos instantes (end por defecto: ahora)
  prom query <promql> [--time T]                consulta instantánea (JSON)
  prom range <promql> --start T --end T --step 1s
  sample --interval 2s --duration 120s --out f.csv [--replicas [name=]ns/deploy]... [--prom name=<promql>]...
  http-get [--method POST] ns/svc:port/path     JSON de un endpoint vía port-forward (también ns/pod/<pod>:port/path)
  sql ns/statefulset "<query>" [--json]         ejecuta SQL con psql dentro del pod
  quantile --file x.csv --col c --q 0.5,0.95,0.99
  trace <id>... [--namespaces a,b] [--since 10m] [--out f.json] [--json]
  assert --criteria criteria.yaml --data <dir> --out result.json
  plot --csv x.csv --x ts [--y c1,c2] --out fig.png
  report --results experiments/results --out experiments/results/summary.md

T = RFC3339, epoch en segundos, "now" o relativo ("-5m").
`

// Run ejecuta expctl con args (sin el nombre del programa) y devuelve el código de salida.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, kube Kubectl) int {
	if kube == nil {
		kube = NewKube(os.Getenv("KUBE_CONTEXT"))
	}
	if len(args) == 0 {
		_, _ = io.WriteString(stderr, usageText)
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "now":
		_, err = fmt.Fprintln(stdout, NowString())
	case "elapsed":
		err = cmdElapsed(rest, stdout)
	case "prom":
		err = cmdProm(ctx, rest, stdout, stderr, kube)
	case "sample":
		err = cmdSample(ctx, rest, stderr, kube)
	case "http-get":
		err = cmdHTTPGet(ctx, rest, stdout, stderr, kube)
	case "sql":
		err = cmdSQL(ctx, rest, stdout, stderr, kube)
	case "quantile":
		err = cmdQuantile(rest, stdout, stderr)
	case "trace":
		err = cmdTrace(ctx, rest, stdout, stderr, kube)
	case "assert":
		var failed bool
		failed, err = cmdAssert(rest, stdout, stderr)
		if err == nil && failed {
			return 1
		}
	case "plot":
		err = cmdPlot(rest, stderr)
	case "report":
		err = cmdReport(rest, stdout, stderr)
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, usageText)
	default:
		_, _ = fmt.Fprintf(stderr, "expctl: subcomando desconocido %q\n\n%s", cmd, usageText)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "expctl %s: %v\n", cmd, err)
		return 2
	}
	_, _ = fmt.Fprintf(stderr, "expctl %s: %v\n", cmd, err)
	return 1
}

func cmdProm(ctx context.Context, args []string, stdout, stderr io.Writer, kube Kubectl) error {
	if len(args) == 0 || (args[0] != "query" && args[0] != "range") {
		return usageErr("uso: prom query <promql> [--time T] | prom range <promql> --start T --end T --step 1s")
	}
	mode := args[0]
	fs := newFlagSet("prom "+mode, stderr)
	at := fs.String("time", "now", "instante de la consulta")
	start := fs.String("start", "", "inicio del rango")
	end := fs.String("end", "now", "fin del rango")
	step := fs.String("step", "1s", "paso del rango (duración o segundos)")
	out := fs.String("out", "", "archivo de salida (por defecto stdout)")
	url := fs.String("url", "", "URL base de Prometheus (omite el port-forward)")
	pos, err := parseFlags(fs, args[1:])
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("falta la consulta PromQL (un solo argumento, entre comillas)")
	}
	now := time.Now()
	sess := &PromSession{Kube: kube, BaseURL: *url}
	defer sess.Close()

	var raw []byte
	switch mode {
	case "query":
		t, err := ParseTime(*at, now)
		if err != nil {
			return usageErr("%v", err)
		}
		raw, err = sess.Do(ctx, func(c *PromClient) ([]byte, error) { return c.Query(ctx, pos[0], t) })
		if err != nil {
			return err
		}
	case "range":
		if *start == "" {
			return usageErr("prom range requiere --start")
		}
		s, err := ParseTime(*start, now)
		if err != nil {
			return usageErr("%v", err)
		}
		e, err := ParseTime(*end, now)
		if err != nil {
			return usageErr("%v", err)
		}
		st, err := parseStep(*step)
		if err != nil {
			return usageErr("%v", err)
		}
		raw, err = sess.Do(ctx, func(c *PromClient) ([]byte, error) { return c.QueryRange(ctx, pos[0], s, e, st) })
		if err != nil {
			return err
		}
	}
	return writeOut(*out, stdout, append(raw, '\n'))
}

// parseStep acepta "1s", "500ms" o segundos decimales ("0.5").
func parseStep(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d, nil
	}
	var f float64
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%g", &f); err == nil && f > 0 {
		return time.Duration(f * float64(time.Second)), nil
	}
	return 0, fmt.Errorf("--step inválido %q", s)
}

// writeOut escribe data en path o, si path está vacío, en stdout.
func writeOut(path string, stdout io.Writer, data []byte) error {
	if path == "" {
		_, err := stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func cmdQuantile(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("quantile", stderr)
	file := fs.String("file", "", "CSV de entrada")
	col := fs.String("col", "", "columna")
	qs := fs.String("q", "0.5,0.95,0.99", "cuantiles (0..1) separados por coma")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *file == "" || *col == "" {
		return usageErr("uso: quantile --file x.csv --col c --q 0.5,0.95,0.99")
	}
	t, err := ReadCSV(*file)
	if err != nil {
		return err
	}
	vals, err := t.Floats(*col)
	if err != nil {
		return err
	}
	res := map[string]float64{}
	for _, q := range strings.Split(*qs, ",") {
		q = strings.TrimSpace(q)
		var f float64
		if _, err := fmt.Sscanf(q, "%g", &f); err != nil {
			return usageErr("cuantil inválido %q", q)
		}
		v, err := Percentile(vals, f)
		if err != nil {
			return err
		}
		res[q] = v
	}
	return writeJSON(stdout, map[string]any{"file": *file, "col": *col, "n": len(vals), "quantiles": res})
}

func cmdElapsed(args []string, stdout io.Writer) error {
	if len(args) < 1 || len(args) > 2 {
		return usageErr("uso: elapsed <start> [end]")
	}
	now := time.Now()
	start, err := ParseTime(args[0], now)
	if err != nil {
		return usageErr("%v", err)
	}
	end := now
	if len(args) == 2 {
		if end, err = ParseTime(args[1], now); err != nil {
			return usageErr("%v", err)
		}
	}
	_, err = fmt.Fprintf(stdout, "%.3f\n", end.Sub(start).Seconds())
	return err
}
