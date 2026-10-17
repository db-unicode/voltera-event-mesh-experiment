// Package envcfg lee configuración de variables de entorno acumulando los errores:
// el servicio informa todos los problemas de config de una vez y no arranca.
package envcfg

import (
	"errors"
	"fmt"
	"strconv"
)

// Reader lee variables con getenv (os.Getenv en producción, un mapa en tests).
type Reader struct {
	get  func(string) string
	errs []error
}

// New crea un Reader.
func New(getenv func(string) string) *Reader { return &Reader{get: getenv} }

// Err devuelve todos los errores acumulados (nil si no hubo).
func (r *Reader) Err() error { return errors.Join(r.errs...) }

func (r *Reader) fail(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf(format, args...))
}

// String devuelve name o def si está vacía.
func (r *Reader) String(name, def string) string {
	if v := r.get(name); v != "" {
		return v
	}
	return def
}

// Required devuelve name; si está vacía acumula un error.
func (r *Reader) Required(name string) string {
	v := r.get(name)
	if v == "" {
		r.fail("%s is required", name)
	}
	return v
}

// Region devuelve REGION y exige r1 o r2.
func (r *Reader) Region() string {
	v := r.get("REGION")
	if v != "r1" && v != "r2" {
		r.fail("REGION must be r1 or r2, got %q", v)
	}
	return v
}

// Int devuelve name como entero >= minV (def si está vacía).
func (r *Reader) Int(name string, def, minV int) int {
	v := r.get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < minV {
		r.fail("%s must be an integer >= %d, got %q", name, minV, v)
		return def
	}
	return n
}

// Int64 es como Int para int64 (sin mínimo).
func (r *Reader) Int64(name string, def int64) int64 {
	v := r.get(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		r.fail("%s must be an integer, got %q", name, v)
		return def
	}
	return n
}

// Float devuelve name como número >= minV (def si está vacía).
func (r *Reader) Float(name string, def, minV float64) float64 {
	v := r.get(name)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < minV {
		r.fail("%s must be a number >= %v, got %q", name, minV, v)
		return def
	}
	return f
}

// JoinErrs une los errores no nil de errs (nil si no hay ninguno).
func JoinErrs(errs ...error) error { return errors.Join(errs...) }
