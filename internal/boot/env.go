// Package boot reúne lo común del arranque de los servicios: lectura validada de
// variables de entorno, conexión a Postgres/Redis y servidor de operación.
package boot

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Env lee variables de entorno acumulando los errores: el servicio llama a Err una
// vez y falla al arrancar con todos los problemas juntos.
type Env struct {
	get  func(string) string
	errs []error
}

// NewEnv crea un lector sobre get (os.Getenv en producción, un mapa en tests).
func NewEnv(get func(string) string) *Env { return &Env{get: get} }

// Err devuelve los errores acumulados (nil si todo es válido).
func (e *Env) Err() error { return errors.Join(e.errs...) }

func (e *Env) fail(format string, args ...any) { e.errs = append(e.errs, fmt.Errorf(format, args...)) }

// String devuelve la variable o def si está vacía.
func (e *Env) String(name, def string) string {
	if v := strings.TrimSpace(e.get(name)); v != "" {
		return v
	}
	return def
}

// Required devuelve la variable; si falta, acumula el error.
func (e *Env) Required(name string) string {
	v := strings.TrimSpace(e.get(name))
	if v == "" {
		e.fail("%s is required", name)
	}
	return v
}

// Int devuelve un entero >= lo (def si la variable está vacía).
func (e *Env) Int(name string, def, lo int) int {
	v := strings.TrimSpace(e.get(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo {
		e.fail("%s must be an integer >= %d, got %q", name, lo, v)
		return def
	}
	return n
}

// Seconds devuelve una duración dada en segundos enteros >= lo.
func (e *Env) Seconds(name string, def, lo int) time.Duration {
	return time.Duration(e.Int(name, def, lo)) * time.Second
}

// Region devuelve REGION validada como r1 o r2.
func (e *Env) Region() string {
	r := e.Required("REGION")
	if r != "" && r != "r1" && r != "r2" {
		e.fail("REGION must be r1 or r2, got %q", r)
	}
	return r
}

// HostPorts devuelve una lista "host:port,host:port" validada (def si está vacía).
func (e *Env) HostPorts(name, def string) []string {
	raw := e.String(name, def)
	var out []string
	for _, b := range strings.Split(raw, ",") {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(b); err != nil {
			e.fail("%s: invalid address %q: %v", name, b, err)
			continue
		}
		out = append(out, b)
	}
	if len(out) == 0 {
		e.fail("%s: no addresses", name)
	}
	return out
}

// Addr devuelve una dirección de escucha ":puerto" o "host:puerto" validada.
func (e *Env) Addr(name, def string) string {
	v := e.String(name, def)
	if _, _, err := net.SplitHostPort(v); err != nil {
		e.fail("%s: invalid address %q: %v", name, v, err)
	}
	return v
}
