package expctl

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// tsLayout es RFC3339 con 9 decimales fijos: parsea como RFC3339Nano y ordena como texto.
const tsLayout = "2006-01-02T15:04:05.000000000Z"

// NowString devuelve la hora actual en RFC3339Nano UTC (9 decimales fijos).
func NowString() string { return time.Now().UTC().Format(tsLayout) }

// ParseTime acepta RFC3339(Nano), epoch en segundos (admite decimales), "now" o un
// desplazamiento relativo a ahora como "-5m".
func ParseTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "" || s == "now":
		return now, nil
	case strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+"):
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, fmt.Errorf("tiempo relativo inválido %q", s)
		}
		return now.Add(d), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(f)
		return time.Unix(sec, int64((f-float64(sec))*1e9)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("tiempo inválido %q (usa RFC3339, epoch, now o -5m)", s)
}

// parseFlags permite flags antes y después de los argumentos posicionales.
func parseFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// newFlagSet crea un FlagSet que escribe errores y ayuda en w.
func newFlagSet(name string, w io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(w)
	return fs
}

// stringList es un flag repetible.
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// writeJSON escribe v indentado.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// splitNS separa "ns/resto" en namespace y resto.
func splitNS(s string) (string, string, error) {
	ns, rest, ok := strings.Cut(s, "/")
	if !ok || ns == "" || rest == "" {
		return "", "", fmt.Errorf("se esperaba ns/nombre, recibido %q", s)
	}
	return ns, rest, nil
}

var errUsage = errors.New("uso incorrecto")

func usageErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, a...))
}
