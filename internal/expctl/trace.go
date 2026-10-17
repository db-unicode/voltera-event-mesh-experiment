package expctl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

const defaultTraceNamespaces = "voltera-r1,voltera-r2,voltera-global,voltera-infra"

// Contenedores de Istio que no emiten logs de aplicación.
var istioContainers = map[string]bool{"istio-proxy": true, "istio-init": true, "istio-validation": true}

// Hop es una línea de log JSON que menciona el id seguido.
type Hop struct {
	TS        time.Time       `json:"ts"`
	OffsetMs  float64         `json:"offsetMs"`
	DeltaMs   float64         `json:"deltaMs"`
	Service   string          `json:"service"`
	Namespace string          `json:"namespace"`
	Pod       string          `json:"pod"`
	Container string          `json:"container"`
	Stage     string          `json:"stage,omitempty"`
	Result    string          `json:"result,omitempty"`
	EventID   string          `json:"eventId,omitempty"`
	TraceID   string          `json:"traceId,omitempty"`
	Msg       string          `json:"msg,omitempty"`
	Line      json.RawMessage `json:"line"`
}

type podRef struct {
	Namespace, Name string
	Containers      []string
	Restarted       map[string]bool
}

// Tracer junta logs de pods y reconstruye la secuencia de un evento.
type Tracer struct {
	Kube       Kubectl
	Namespaces []string
	Since      string
	IDs        []string
	Warn       io.Writer
}

func (t *Tracer) warn(format string, a ...any) {
	if t.Warn != nil {
		_, _ = fmt.Fprintf(t.Warn, "trace: "+format+"\n", a...)
	}
}

func (t *Tracer) listPods(ctx context.Context, ns string) ([]podRef, error) {
	out, err := t.Kube.Output(ctx, "-n", ns, "get", "pods", "-o", "json")
	if err != nil {
		return nil, err
	}
	var pl struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name string `json:"name"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				ContainerStatuses []struct {
					Name         string `json:"name"`
					RestartCount int    `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &pl); err != nil {
		return nil, err
	}
	var pods []podRef
	for _, it := range pl.Items {
		p := podRef{Namespace: ns, Name: it.Metadata.Name, Restarted: map[string]bool{}}
		for _, c := range it.Spec.Containers {
			if !istioContainers[c.Name] {
				p.Containers = append(p.Containers, c.Name)
			}
		}
		for _, cs := range it.Status.ContainerStatuses {
			p.Restarted[cs.Name] = cs.RestartCount > 0
		}
		pods = append(pods, p)
	}
	return pods, nil
}

func (t *Tracer) matches(line []byte) bool {
	for _, id := range t.IDs {
		if bytes.Contains(line, []byte(id)) {
			return true
		}
	}
	return false
}

// ParseLogLine interpreta una línea de `kubectl logs --timestamps`. Devuelve ok=false
// si no es JSON (las líneas de texto se ignoran).
func ParseLogLine(line string) (kubeTS time.Time, fields map[string]any, raw []byte, ok bool) {
	tsStr, rest, found := strings.Cut(line, " ")
	if !found {
		return time.Time{}, nil, nil, false
	}
	kubeTS, _ = time.Parse(time.RFC3339Nano, tsStr)
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "{") {
		return kubeTS, nil, nil, false
	}
	if err := json.Unmarshal([]byte(rest), &fields); err != nil {
		return kubeTS, nil, nil, false
	}
	return kubeTS, fields, []byte(rest), true
}

func str(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func (t *Tracer) hopsFromLogs(p podRef, container string, logs []byte) (hops []Hop, skipped int) {
	sc := bufio.NewScanner(bytes.NewReader(logs))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		kubeTS, fields, raw, ok := ParseLogLine(line)
		if !ok {
			skipped++
			continue
		}
		if !t.matches(raw) {
			continue
		}
		ts := kubeTS
		if s := str(fields, "ts"); s != "" {
			if pt, err := time.Parse(time.RFC3339Nano, s); err == nil {
				ts = pt
			}
		}
		if ts.IsZero() {
			skipped++
			continue
		}
		svc := str(fields, "service")
		if svc == "" {
			svc = container
		}
		hops = append(hops, Hop{
			TS: ts.UTC(), Service: svc, Namespace: p.Namespace, Pod: p.Name, Container: container,
			Stage: str(fields, "stage"), Result: str(fields, "result"),
			EventID: str(fields, "eventId"), TraceID: str(fields, "traceId"), Msg: str(fields, "msg"),
			Line: json.RawMessage(raw),
		})
	}
	return hops, skipped
}

// Collect lee los logs de todos los pods y devuelve los hops ordenados por ts.
func (t *Tracer) Collect(ctx context.Context) ([]Hop, error) {
	type job struct {
		pod       podRef
		container string
		previous  bool
	}
	var jobs []job
	for _, ns := range t.Namespaces {
		pods, err := t.listPods(ctx, ns)
		if err != nil {
			return nil, err
		}
		for _, p := range pods {
			for _, c := range p.Containers {
				jobs = append(jobs, job{p, c, false})
				if p.Restarted[c] {
					jobs = append(jobs, job{p, c, true})
				}
			}
		}
	}
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		hops    []Hop
		skipped int
		sem     = make(chan struct{}, 8)
	)
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			args := []string{"-n", j.pod.Namespace, "logs", j.pod.Name, "-c", j.container, "--timestamps"}
			if t.Since != "" {
				args = append(args, "--since", t.Since)
			}
			if j.previous {
				args = append(args, "--previous")
			}
			out, err := t.Kube.Output(ctx, args...)
			if err != nil {
				if !j.previous {
					t.warn("%v", err)
				}
				return
			}
			h, sk := t.hopsFromLogs(j.pod, j.container, out)
			mu.Lock()
			hops = append(hops, h...)
			skipped += sk
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.SliceStable(hops, func(a, b int) bool {
		if !hops[a].TS.Equal(hops[b].TS) {
			return hops[a].TS.Before(hops[b].TS)
		}
		if hops[a].Namespace != hops[b].Namespace {
			return hops[a].Namespace < hops[b].Namespace
		}
		return hops[a].Pod < hops[b].Pod
	})
	for i := range hops {
		hops[i].OffsetMs = float64(hops[i].TS.Sub(hops[0].TS).Nanoseconds()) / 1e6
		if i > 0 {
			hops[i].DeltaMs = float64(hops[i].TS.Sub(hops[i-1].TS).Nanoseconds()) / 1e6
		}
	}
	if skipped > 0 {
		t.warn("%d líneas ignoradas (no JSON o sin ts)", skipped)
	}
	return hops, nil
}

// WriteTable imprime la tabla ts | Δms | service | pod | stage | result.
func WriteTable(w io.Writer, hops []Hop) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ts\tΔms\tservice\tpod\tstage\tresult")
	for _, h := range hops {
		_, _ = fmt.Fprintf(tw, "%s\t%.3f\t%s\t%s\t%s\t%s\n",
			h.TS.Format("2006-01-02T15:04:05.000000Z07:00"), h.DeltaMs, h.Service, h.Pod, h.Stage, h.Result)
	}
	_ = tw.Flush()
}

func cmdTrace(ctx context.Context, args []string, stdout, stderr io.Writer, kube Kubectl) error {
	fs := newFlagSet("trace", stderr)
	nss := fs.String("namespaces", defaultTraceNamespaces, "namespaces separados por coma")
	since := fs.String("since", "10m", "ventana de logs (formato de kubectl logs --since)")
	out := fs.String("out", "", "guarda el JSON en este archivo")
	asJSON := fs.Bool("json", false, "imprime JSON en stdout en vez de la tabla")
	ids, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return usageErr("uso: trace <eventId|matchId>... [--namespaces a,b] [--since 10m]")
	}
	tr := &Tracer{Kube: kube, Since: *since, IDs: ids, Warn: stderr}
	for _, ns := range strings.Split(*nss, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			tr.Namespaces = append(tr.Namespaces, ns)
		}
	}
	hops, err := tr.Collect(ctx)
	if err != nil {
		return err
	}
	res := map[string]any{"ids": ids, "hops": hops, "count": len(hops)}
	if len(hops) > 0 {
		res["totalMs"] = hops[len(hops)-1].OffsetMs
		res["services"] = distinctServices(hops)
	}
	if *out != "" {
		var buf bytes.Buffer
		if err := writeJSON(&buf, res); err != nil {
			return err
		}
		if err := writeOut(*out, stdout, buf.Bytes()); err != nil {
			return err
		}
	}
	if *asJSON {
		return writeJSON(stdout, res)
	}
	WriteTable(stdout, hops)
	if len(hops) == 0 {
		_, _ = fmt.Fprintln(stderr, "trace: sin coincidencias")
	}
	return nil
}

func distinctServices(hops []Hop) []string {
	seen := map[string]bool{}
	var out []string
	for _, h := range hops {
		if !seen[h.Service] {
			seen[h.Service] = true
			out = append(out, h.Service)
		}
	}
	sort.Strings(out)
	return out
}
