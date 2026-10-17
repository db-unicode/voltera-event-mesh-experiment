// zt-probe intenta cada celda de la matriz Zero Trust (E8, E9) y dice si el bus la permitió o la rechazó.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/ztprobe"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(ztprobe.Run(ctx, os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr))
}
