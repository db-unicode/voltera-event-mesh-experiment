package transfer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/boot"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/kafkax"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/failpoint"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/inbox"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/migrate"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/telemetry"
)

const serviceName = "energy-transfer"

// Config se lee de variables de entorno y se valida al arrancar.
type Config struct {
	Region         string
	DatabaseURL    string
	KafkaBrokers   []string
	Group          string
	DeviceURL      string
	CommandKey     []byte
	CommandTimeout time.Duration
	OpsAddr        string
}

// Topic es el topic de entrada de la región.
func (c Config) Topic() string { return c.Region + ".energy-matched" }

// LoadConfig lee la configuración. Vars: REGION, DATABASE_URL, KAFKA_BROKERS,
// KAFKA_GROUP (energy-transfer), DEVICE_SIM_URL, DEVICE_COMMAND_KEY,
// COMMAND_TIMEOUT_S (5), OPS_ADDR (:9090).
func LoadConfig(getenv func(string) string) (Config, error) {
	e := boot.NewEnv(getenv)
	c := Config{
		Region:         e.Region(),
		DatabaseURL:    e.Required("DATABASE_URL"),
		KafkaBrokers:   e.HostPorts("KAFKA_BROKERS", "kafka.voltera-infra.svc:9092"),
		Group:          e.String("KAFKA_GROUP", ConsumerName),
		DeviceURL:      e.Required("DEVICE_SIM_URL"),
		CommandKey:     []byte(e.Required("DEVICE_COMMAND_KEY")),
		CommandTimeout: e.Seconds("COMMAND_TIMEOUT_S", 5, 1),
		OpsAddr:        e.Addr("OPS_ADDR", ":9090"),
	}
	return c, e.Err()
}

// Run arranca el servicio y bloquea hasta que ctx se cancele. Escala por lag de Kafka
// (KEDA, 0 a 6 réplicas): cada réplica es miembro del grupo y procesa sus particiones.
func Run(ctx context.Context, getenv func(string) string) error {
	cfg, err := LoadConfig(getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	fp, err := failpoint.FromEnv()
	if err != nil {
		return fmt.Errorf("failpoints: %w", err)
	}
	svc := telemetry.New(telemetry.InfoFromEnv(serviceName), slog.Default())
	svc.Failpoints = fp
	log := svc.Logger
	go func() {
		if err := svc.Serve(ctx, cfg.OpsAddr); err != nil {
			log.Error("ops server failed", "stage", "start", "result", "error", "error", err)
		}
	}()

	pool, err := boot.ConnectPG(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool, inbox.Migrations(), Migrations()); err != nil {
		return err
	}
	svc.AddReadyCheck("postgres", pool.Ping)
	svc.AddState("transfers", func(ctx context.Context) any {
		c, err := StatusCounts(ctx, pool)
		if err != nil {
			return map[string]string{"error": err.Error()}
		}
		return c
	})

	h := &Handler{
		Pool: pool, Svc: svc, Metrics: telemetry.NewDomainMetrics(svc.Reg), FP: fp, Log: log, Timeout: cfg.CommandTimeout,
		Sender: HTTPSender{BaseURL: cfg.DeviceURL, Key: cfg.CommandKey, Client: &http.Client{}},
	}
	cons, err := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers: cfg.KafkaBrokers, ClientID: serviceName, Group: cfg.Group, Topic: cfg.Topic(),
		Logger: log, Registerer: svc.Reg,
	})
	if err != nil {
		return err
	}
	svc.AddReadyCheck("kafka", cons.Ping)

	log.Info("energy-transfer started", "stage", "start", "result", "ok", "region", cfg.Region,
		"topic", cfg.Topic(), "group", cfg.Group, "device", cfg.DeviceURL)
	err = cons.Run(ctx, h.Handle)
	cons.Close()
	log.Info("energy-transfer stopped", "stage", "stop", "result", "ok")
	return err
}
