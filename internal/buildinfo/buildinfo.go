// Package buildinfo expone la versión del binario, inyectada con -ldflags.
package buildinfo

// Version se sobrescribe en build: -ldflags "-X .../internal/buildinfo.Version=<sha>".
var Version = "dev"
