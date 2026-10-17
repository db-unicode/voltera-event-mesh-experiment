package boot

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ConnectPG abre el pool y reintenta hasta que la base acepte conexiones o venza ctx.
func ConnectPG(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("postgres not reachable: %w (last: %v)", ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// NewRedis crea el cliente de la vista (la conexión es perezosa; el readyz la comprueba).
func NewRedis(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: password, DialTimeout: 5 * time.Second})
}

// Sleep espera d o hasta que ctx termine; devuelve false si ctx terminó.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
