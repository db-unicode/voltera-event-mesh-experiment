package expctl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// httpTarget es un endpoint dentro del cluster accesible por port-forward.
type httpTarget struct {
	Namespace string
	Resource  string // svc/<n>, pod/<n> o deploy/<n>
	Port      int
	Path      string // empieza por "/"
}

// parseHTTPTarget interpreta ns/svc:port/path o ns/<pod|svc|deploy>/<nombre>:port/path.
func parseHTTPTarget(s string) (httpTarget, error) {
	var t httpTarget
	ns, rest, err := splitNS(s)
	if err != nil {
		return t, err
	}
	t.Namespace = ns
	hostport, path, _ := strings.Cut(rest, "/")
	kind := "svc"
	// forma con tipo explícito: pod/<nombre>:port/path
	if k, after, ok := strings.Cut(rest, "/"); ok && (k == "pod" || k == "svc" || k == "deploy" || k == "deployment") {
		kind = k
		hostport, path, _ = strings.Cut(after, "/")
	}
	name, portStr, ok := strings.Cut(hostport, ":")
	if !ok || name == "" {
		return t, fmt.Errorf("destino %q: se esperaba ns/svc:port/path", s)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return t, fmt.Errorf("destino %q: puerto inválido", s)
	}
	t.Resource = kind + "/" + name
	t.Port = port
	t.Path = "/" + path
	return t, nil
}

func cmdHTTPGet(ctx context.Context, args []string, stdout, stderr io.Writer, kube Kubectl) error {
	fs := newFlagSet("http-get", stderr)
	method := fs.String("method", http.MethodGet, "método HTTP (p. ej. POST para /admin/failpoint)")
	out := fs.String("out", "", "archivo de salida (por defecto stdout)")
	timeout := fs.Duration("timeout", 15*time.Second, "timeout de la petición")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("uso: http-get ns/svc:port/path (p. ej. voltera-r1/device-sim:9090/state)")
	}
	t, err := parseHTTPTarget(pos[0])
	if err != nil {
		return usageErr("%v", err)
	}
	body, err := doHTTP(ctx, kube, t, strings.ToUpper(*method), *timeout)
	if err != nil {
		return err
	}
	if len(body) > 0 && body[len(body)-1] != '\n' {
		body = append(body, '\n')
	}
	return writeOut(*out, stdout, body)
}

func doHTTP(ctx context.Context, kube Kubectl, t httpTarget, method string, timeout time.Duration) ([]byte, error) {
	f, err := kube.PortForward(ctx, t.Namespace, t.Resource, t.Port)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return httpDo(ctx, fmt.Sprintf("http://127.0.0.1:%d%s", f.LocalPort, t.Path), method, timeout)
}

func httpDo(ctx context.Context, url, method string, timeout time.Duration) ([]byte, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %.200s", method, url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}
