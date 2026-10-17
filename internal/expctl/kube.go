package expctl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Rango de puertos locales reservado para los port-forward de expctl.
const (
	forwardPortMin = 19700
	forwardPortMax = 19799
)

// Forward es un `kubectl port-forward` abierto hacia un recurso del cluster.
type Forward struct {
	LocalPort int
	cancel    context.CancelFunc
	done      chan struct{}
	release   func()
}

// Close termina el port-forward y libera el puerto local.
func (f *Forward) Close() {
	f.cancel()
	<-f.done
	f.release()
}

// Kubectl abstrae kubectl para poder simularlo en tests.
type Kubectl interface {
	// Output ejecuta kubectl con el contexto fijado y devuelve stdout.
	Output(ctx context.Context, args ...string) ([]byte, error)
	// PortForward abre un port-forward a recurso (p. ej. "svc/prometheus") en el namespace.
	PortForward(ctx context.Context, namespace, resource string, remotePort int) (*Forward, error)
}

// Kube es la implementación real: `kubectl --context <Context> ...`.
type Kube struct {
	Context string
	Bin     string
}

// NewKube devuelve un Kube para el contexto dado ("" = kind-voltera).
func NewKube(context string) *Kube {
	if context == "" {
		context = "kind-voltera"
	}
	return &Kube{Context: context, Bin: "kubectl"}
}

// Output implementa Kubectl.
func (k *Kube) Output(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{"--context", k.Context}, args...)
	cmd := exec.CommandContext(ctx, k.Bin, full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

var (
	portMu    sync.Mutex
	portTaken = map[int]bool{}
)

// reservePort elige un puerto libre del rango 19700-19799 y lo marca como usado en este proceso.
func reservePort() (int, error) {
	portMu.Lock()
	defer portMu.Unlock()
	span := forwardPortMax - forwardPortMin + 1
	start := rand.IntN(span)
	for i := range span {
		p := forwardPortMin + (start+i)%span
		if portTaken[p] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			continue
		}
		_ = l.Close()
		portTaken[p] = true
		return p, nil
	}
	return 0, errors.New("sin puertos libres en 19700-19799")
}

func releasePort(p int) {
	portMu.Lock()
	delete(portTaken, p)
	portMu.Unlock()
}

// PortForward implementa Kubectl. Espera a que kubectl anuncie "Forwarding from".
func (k *Kube) PortForward(ctx context.Context, namespace, resource string, remotePort int) (*Forward, error) {
	var lastErr error
	for range 5 {
		port, err := reservePort()
		if err != nil {
			return nil, err
		}
		f, err := k.startForward(ctx, namespace, resource, port, remotePort)
		if err == nil {
			return f, nil
		}
		releasePort(port)
		lastErr = err
	}
	return nil, lastErr
}

func (k *Kube) startForward(ctx context.Context, namespace, resource string, local, remote int) (*Forward, error) {
	cctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(cctx, k.Bin, "--context", k.Context, "-n", namespace,
		"port-forward", "--address", "127.0.0.1", resource, fmt.Sprintf("%d:%d", local, remote))
	cmd.WaitDelay = 2 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		signaled := false
		for sc.Scan() {
			if !signaled && strings.HasPrefix(sc.Text(), "Forwarding from") {
				signaled = true
				close(ready)
			}
		}
	}()
	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
		close(done)
	}()
	select {
	case <-ready:
		return &Forward{LocalPort: local, cancel: cancel, done: done, release: func() { releasePort(local) }}, nil
	case err := <-exited:
		cancel()
		return nil, fmt.Errorf("port-forward %s/%s: %v: %s", namespace, resource, err, strings.TrimSpace(stderr.String()))
	case <-time.After(20 * time.Second):
		cancel()
		<-done
		return nil, fmt.Errorf("port-forward %s/%s: timeout esperando conexión", namespace, resource)
	case <-ctx.Done():
		cancel()
		<-done
		return nil, ctx.Err()
	}
}
