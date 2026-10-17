package expctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	promNamespace = "istio-system"
	promResource  = "svc/prometheus"
	promPort      = 9090
)

// promAPIError es un error devuelto por Prometheus (consulta inválida, timeout); no se reintenta.
type promAPIError struct{ Type, Msg string }

func (e *promAPIError) Error() string { return fmt.Sprintf("prometheus %s: %s", e.Type, e.Msg) }

// PromClient consulta la API HTTP de Prometheus.
type PromClient struct {
	BaseURL string
	HTTP    *http.Client
}

func (c *PromClient) get(ctx context.Context, path string, q url.Values) ([]byte, error) {
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var env struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("prometheus HTTP %d: respuesta no JSON: %.200s", resp.StatusCode, body)
	}
	if env.Status != "success" {
		return nil, &promAPIError{Type: env.ErrorType, Msg: env.Error}
	}
	return body, nil
}

// Query ejecuta una consulta instantánea y devuelve el JSON crudo de la API.
func (c *PromClient) Query(ctx context.Context, query string, at time.Time) ([]byte, error) {
	q := url.Values{"query": {query}}
	if !at.IsZero() {
		q.Set("time", strconv.FormatFloat(float64(at.UnixNano())/1e9, 'f', 3, 64))
	}
	return c.get(ctx, "/api/v1/query", q)
}

// QueryRange ejecuta una consulta de rango y devuelve el JSON crudo de la API.
func (c *PromClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]byte, error) {
	q := url.Values{
		"query": {query},
		"start": {strconv.FormatFloat(float64(start.UnixNano())/1e9, 'f', 3, 64)},
		"end":   {strconv.FormatFloat(float64(end.UnixNano())/1e9, 'f', 3, 64)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	return c.get(ctx, "/api/v1/query_range", q)
}

// FirstValue extrae el primer valor de un resultado instantáneo (vector o escalar).
// Devuelve "" si el resultado está vacío.
func FirstValue(raw []byte) (string, error) {
	var r struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", err
	}
	pick := func(v []json.RawMessage) (string, error) {
		if len(v) != 2 {
			return "", fmt.Errorf("muestra inesperada")
		}
		var s string
		if err := json.Unmarshal(v[1], &s); err != nil {
			return "", err
		}
		return s, nil
	}
	switch r.Data.ResultType {
	case "scalar":
		var v []json.RawMessage
		if err := json.Unmarshal(r.Data.Result, &v); err != nil {
			return "", err
		}
		return pick(v)
	case "vector":
		var vec []struct {
			Value []json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(r.Data.Result, &vec); err != nil {
			return "", err
		}
		if len(vec) == 0 {
			return "", nil
		}
		return pick(vec[0].Value)
	}
	return "", fmt.Errorf("resultType %q no soportado", r.Data.ResultType)
}

// PromSession mantiene un port-forward a Prometheus y lo reabre si falla una consulta.
type PromSession struct {
	Kube Kubectl
	// BaseURL, si no está vacío, evita el port-forward (tests o Prometheus ya accesible).
	BaseURL string

	mu  sync.Mutex
	fwd *Forward
}

// Close cierra el port-forward.
func (s *PromSession) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fwd != nil {
		s.fwd.Close()
		s.fwd = nil
	}
}

func (s *PromSession) client(ctx context.Context) (*PromClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.BaseURL != "" {
		return &PromClient{BaseURL: s.BaseURL}, nil
	}
	if s.fwd == nil {
		f, err := s.Kube.PortForward(ctx, promNamespace, promResource, promPort)
		if err != nil {
			return nil, err
		}
		s.fwd = f
	}
	return &PromClient{BaseURL: fmt.Sprintf("http://127.0.0.1:%d", s.fwd.LocalPort)}, nil
}

func (s *PromSession) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fwd != nil {
		s.fwd.Close()
		s.fwd = nil
	}
}

// Do ejecuta fn; si falla y hay port-forward, lo reabre y reintenta una vez.
func (s *PromSession) Do(ctx context.Context, fn func(*PromClient) ([]byte, error)) ([]byte, error) {
	c, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	out, err := fn(c)
	var apiErr *promAPIError
	if err == nil || s.BaseURL != "" || ctx.Err() != nil || errors.As(err, &apiErr) {
		return out, err
	}
	s.reset()
	if c, err = s.client(ctx); err != nil {
		return nil, err
	}
	return fn(c)
}
