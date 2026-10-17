package expctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// runInfo es la última corrida (latest) de un experimento.
type runInfo struct {
	Experiment string
	Dir        string // ruta absoluta de la carpeta de la corrida
	Name       string // nombre de la carpeta <UTC>-<sha>-r<rep>
	Date       string
	SHA        string
	Dirty      bool
	Result     AssertResult
}

// parseRunName separa "<UTC>-<sha>[-dirty]-r<rep>".
func parseRunName(name string) (date, sha string) {
	parts := strings.Split(name, "-")
	if len(parts) < 2 {
		return name, ""
	}
	date = parts[0]
	if t, err := time.Parse("20060102T150405Z", parts[0]); err == nil {
		date = t.Format("2006-01-02 15:04:05 UTC")
	}
	rest := parts[1:]
	if n := len(rest); n > 0 && strings.HasPrefix(rest[n-1], "r") {
		rest = rest[:n-1]
	}
	return date, strings.Join(rest, "-")
}

func loadRun(resultsDir, exp string) (*runInfo, error) {
	link, err := os.Readlink(filepath.Join(resultsDir, exp, "latest"))
	if err != nil {
		return nil, err
	}
	// no se usa EvalSymlinks para que los enlaces relativos no dependan de /private en macOS
	target, err := filepath.Abs(filepath.Join(resultsDir, exp, link))
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(target, "result.json"))
	if err != nil {
		return nil, err
	}
	ri := &runInfo{Experiment: exp, Dir: target, Name: filepath.Base(target)}
	if err := json.Unmarshal(raw, &ri.Result); err != nil {
		return nil, fmt.Errorf("%s/result.json: %w", target, err)
	}
	ri.Date, ri.SHA = parseRunName(ri.Name)
	if envRaw, err := os.ReadFile(filepath.Join(target, "env.json")); err == nil {
		var env struct {
			Git struct {
				SHA   string `json:"sha"`
				Dirty bool   `json:"dirty"`
			} `json:"git"`
			StartedAt string `json:"startedAt"`
		}
		if json.Unmarshal(envRaw, &env) == nil {
			if env.Git.SHA != "" {
				ri.SHA = env.Git.SHA
			}
			ri.Dirty = env.Git.Dirty
			if t, err := time.Parse(time.RFC3339Nano, env.StartedAt); err == nil {
				ri.Date = t.UTC().Format("2006-01-02 15:04:05 UTC")
			}
		}
	}
	return ri, nil
}

func cell(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s)
}

func fmtVal(v any) string {
	if v == nil {
		return "-"
	}
	if f, ok := v.(float64); ok {
		return fmt.Sprintf("%.6g", f)
	}
	return fmt.Sprint(v)
}

// BuildReport arma el Markdown con la última corrida de cada experimento.
// Los enlaces son relativos a la carpeta de outPath.
func BuildReport(resultsDir, outPath string, now time.Time) (string, error) {
	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return "", err
	}
	var runs []*runInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if ri, err := loadRun(resultsDir, e.Name()); err == nil {
			runs = append(runs, ri)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].Experiment < runs[j].Experiment })

	absOut, err := filepath.Abs(outPath)
	if err != nil {
		return "", err
	}
	link := func(dir string) string {
		rel, err := filepath.Rel(filepath.Dir(absOut), dir)
		if err != nil {
			return dir
		}
		return filepath.ToSlash(rel) + "/"
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "# Resumen de experimentos\n\nGenerado: %s. Última corrida (`latest`) de cada experimento.\n\n", now.UTC().Format(time.RFC3339))
	if len(runs) == 0 {
		b.WriteString("Sin corridas en " + filepath.ToSlash(resultsDir) + ".\n")
		return b.String(), nil
	}
	b.WriteString("| Experimento | Veredicto | Criterios | Fecha | SHA | Carpeta |\n|---|---|---|---|---|---|\n")
	for _, r := range runs {
		pass := 0
		for _, c := range r.Result.Criteria {
			if c.Verdict == "PASS" {
				pass++
			}
		}
		sha := r.SHA
		if r.Dirty && !strings.HasSuffix(sha, "dirty") {
			sha += " (dirty)"
		}
		fmt.Fprintf(&b, "| %s | %s | %d/%d | %s | `%s` | [%s](%s) |\n", cell(r.Experiment), r.Result.Verdict, pass,
			len(r.Result.Criteria), r.Date, cell(sha), cell(r.Name), link(r.Dir))
	}
	for _, r := range runs {
		fmt.Fprintf(&b, "\n## %s: %s\n\nCorrida [%s](%s)\n\n", r.Experiment, r.Result.Verdict, r.Name, link(r.Dir))
		b.WriteString("| Criterio | Descripción | Observado | Esperado | Veredicto |\n|---|---|---|---|---|\n")
		for _, c := range r.Result.Criteria {
			obs := fmtVal(c.Observed)
			if c.Error != "" {
				obs = "error: " + c.Error
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s %s | %s |\n", cell(c.ID), cell(c.Description), cell(obs),
				opSymbol[c.Op], cell(fmtVal(c.Expected)), c.Verdict)
		}
	}
	return b.String(), nil
}

func cmdReport(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("report", stderr)
	results := fs.String("results", "experiments/results", "carpeta de resultados")
	out := fs.String("out", "", "Markdown de salida (por defecto <results>/summary.md)")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return usageErr("uso: report --results experiments/results --out experiments/results/summary.md")
	}
	if *out == "" {
		*out = filepath.Join(*results, "summary.md")
	}
	md, err := BuildReport(*results, *out, time.Now())
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "report: %s\n", *out)
	return nil
}
