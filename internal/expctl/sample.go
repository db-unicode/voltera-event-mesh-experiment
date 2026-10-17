package expctl

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// replicaSpec describe una columna de réplicas: --replicas [name=]ns/deploy o [name=]ns/kind/name.
type replicaSpec struct {
	Name, Namespace, Resource string
}

func parseReplicaSpec(s string) (replicaSpec, error) {
	var rs replicaSpec
	name, target, hasName := strings.Cut(s, "=")
	if !hasName {
		target = s
	}
	parts := strings.Split(target, "/")
	switch len(parts) {
	case 2:
		rs.Namespace, rs.Resource = parts[0], "deployment/"+parts[1]
	case 3:
		rs.Namespace, rs.Resource = parts[0], parts[1]+"/"+parts[2]
	default:
		return rs, fmt.Errorf("--replicas %q: se esperaba [nombre=]ns/deploy", s)
	}
	if rs.Namespace == "" || strings.HasSuffix(rs.Resource, "/") {
		return rs, fmt.Errorf("--replicas %q inválido", s)
	}
	rs.Name = name
	if !hasName {
		rs.Name = parts[len(parts)-1]
	}
	return rs, nil
}

type promCol struct{ Name, Query string }

func parsePromCol(s string) (promCol, error) {
	name, q, ok := strings.Cut(s, "=")
	if !ok || name == "" || q == "" {
		return promCol{}, fmt.Errorf("--prom %q: se esperaba name=<promql>", s)
	}
	return promCol{Name: strings.TrimSpace(name), Query: q}, nil
}

// Sampler toma muestras periódicas de réplicas y consultas Prometheus.
type Sampler struct {
	Kube     Kubectl
	Prom     *PromSession
	Replicas []replicaSpec
	Proms    []promCol
	Warn     io.Writer
}

// Header devuelve la cabecera del CSV: ts, por cada --replicas <name> (deseadas) y
// <name>_ready (listas), y luego cada --prom.
func (s *Sampler) Header() []string {
	h := []string{"ts"}
	for _, r := range s.Replicas {
		h = append(h, r.Name, r.Name+"_ready")
	}
	for _, p := range s.Proms {
		h = append(h, p.Name)
	}
	return h
}

// replicas lee spec.replicas y status.readyReplicas de un recurso.
func (s *Sampler) replicas(ctx context.Context, r replicaSpec) (desired, ready string, err error) {
	out, err := s.Kube.Output(ctx, "-n", r.Namespace, "get", r.Resource,
		"-o", "jsonpath={.spec.replicas}/{.status.readyReplicas}")
	if err != nil {
		return "", "", err
	}
	d, rd, _ := strings.Cut(strings.TrimSpace(string(out)), "/")
	if d == "" {
		d = "0"
	}
	if rd == "" {
		rd = "0"
	}
	return d, rd, nil
}

// Row toma una muestra con marca de tiempo ts. Las celdas que fallan quedan vacías.
func (s *Sampler) Row(ctx context.Context, ts time.Time) []string {
	row := make([]string, len(s.Header()))
	row[0] = ts.UTC().Format(tsLayout)
	var wg sync.WaitGroup
	warn := func(format string, a ...any) {
		if s.Warn != nil {
			_, _ = fmt.Fprintf(s.Warn, "sample: "+format+"\n", a...)
		}
	}
	for i, r := range s.Replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, rd, err := s.replicas(ctx, r)
			if err != nil {
				warn("%s: %v", r.Name, err)
				return
			}
			row[1+2*i], row[2+2*i] = d, rd
		}()
	}
	base := 1 + 2*len(s.Replicas)
	for i, p := range s.Proms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, err := s.Prom.Do(ctx, func(c *PromClient) ([]byte, error) { return c.Query(ctx, p.Query, time.Time{}) })
			if err != nil {
				warn("%s: %v", p.Name, err)
				return
			}
			v, err := FirstValue(raw)
			if err != nil {
				warn("%s: %v", p.Name, err)
				return
			}
			row[base+i] = v
		}()
	}
	wg.Wait()
	return row
}

// Run escribe la cabecera y una fila por intervalo durante duration (primera muestra en t=0).
// Cada fila se vacía a disco al escribirse.
func (s *Sampler) Run(ctx context.Context, w io.Writer, interval, duration time.Duration) (int, error) {
	cw := csv.NewWriter(w)
	if err := cw.Write(s.Header()); err != nil {
		return 0, err
	}
	deadline := time.Now().Add(duration)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	n := 0
	for {
		// una muestra nunca debe durar más que 3 intervalos (mínimo 5 s)
		sctx, cancel := context.WithTimeout(ctx, max(3*interval, 5*time.Second))
		row := s.Row(sctx, time.Now())
		cancel()
		if err := cw.Write(row); err != nil {
			return n, err
		}
		cw.Flush()
		if err := cw.Error(); err != nil {
			return n, err
		}
		n++
		select {
		case <-ctx.Done():
			return n, nil
		case <-tick.C:
		}
		if !time.Now().Before(deadline) {
			return n, nil
		}
	}
}

func cmdSample(ctx context.Context, args []string, stderr io.Writer, kube Kubectl) error {
	fs := newFlagSet("sample", stderr)
	interval := fs.Duration("interval", 2*time.Second, "intervalo entre muestras")
	duration := fs.Duration("duration", 120*time.Second, "duración total")
	out := fs.String("out", "", "CSV de salida")
	url := fs.String("prom-url", "", "URL base de Prometheus (omite el port-forward)")
	var reps, proms stringList
	fs.Var(&reps, "replicas", "columna de réplicas [name=]ns/deploy (repetible)")
	fs.Var(&proms, "prom", "columna Prometheus name=<promql> (repetible)")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *out == "" || *interval <= 0 || *duration <= 0 {
		return usageErr("uso: sample --interval 2s --duration 120s --out f.csv [--replicas ns/deploy]... [--prom name=<promql>]...")
	}
	if len(reps)+len(proms) == 0 {
		return usageErr("indica al menos un --replicas o --prom")
	}
	s := &Sampler{Kube: kube, Warn: stderr}
	for _, r := range reps {
		rs, err := parseReplicaSpec(r)
		if err != nil {
			return usageErr("%v", err)
		}
		s.Replicas = append(s.Replicas, rs)
	}
	for _, p := range proms {
		pc, err := parsePromCol(p)
		if err != nil {
			return usageErr("%v", err)
		}
		s.Proms = append(s.Proms, pc)
	}
	seen := map[string]bool{}
	for _, h := range s.Header() {
		if seen[h] {
			return usageErr("columna duplicada %q", h)
		}
		seen[h] = true
	}
	if len(s.Proms) > 0 {
		s.Prom = &PromSession{Kube: kube, BaseURL: *url}
		defer s.Prom.Close()
		// abre el port-forward antes de empezar a medir el tiempo
		if _, err := s.Prom.client(ctx); err != nil {
			return err
		}
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	n, err := s.Run(ctx, f, *interval, *duration)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stderr, "sample: %d muestras en %s\n", n, strconv.Quote(*out))
	return nil
}
