package expctl

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// fakeKube simula kubectl: Output delega en handler y PortForward apunta a forwardTo.
type fakeKube struct {
	handler   func(args []string) ([]byte, error)
	forwardTo *httptest.Server

	mu    sync.Mutex
	calls [][]string
}

func (f *fakeKube) Output(_ context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, args)
	f.mu.Unlock()
	if f.handler == nil {
		return nil, errors.New("sin handler")
	}
	return f.handler(args)
}

func (f *fakeKube) PortForward(_ context.Context, _, _ string, _ int) (*Forward, error) {
	if f.forwardTo == nil {
		return nil, errors.New("sin port-forward en el test")
	}
	u, err := url.Parse(f.forwardTo.URL)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	close(done)
	return &Forward{LocalPort: port, cancel: func() {}, done: done, release: func() {}}, nil
}

func hasArgs(args []string, want ...string) bool {
	return strings.Contains(" "+strings.Join(args, " ")+" ", " "+strings.Join(want, " ")+" ")
}
