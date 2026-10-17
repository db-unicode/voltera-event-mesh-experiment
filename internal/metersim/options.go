// Package metersim es el simulador de medidores y generador de carga (S11): publica
// declaraciones en meter-declarations con dos formatos de fabricante, a tasa de llegada
// fija (modelo abierto) y registra qué envió y cuándo.
package metersim

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/db-unicode/voltera-event-mesh-experiment/internal/broker/amqpx"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/envcfg"
	"github.com/db-unicode/voltera-event-mesh-experiment/internal/metering/acl"
)

// Profile es la forma de la carga.
type Profile string

const (
	// ProfileSteady: tasa constante; recorre los medidores en orden, subiendo la versión en cada vuelta.
	ProfileSteady Profile = "steady"
	// ProfilePairs: por zona, pares SURPLUS/DEFICIT de igual kWh (1 match esperado por par).
	ProfilePairs Profile = "pairs"
	// ProfileSpike: rampa lineal de 0 a --rate y meseta hasta el final.
	ProfileSpike Profile = "spike"
	// ProfileDup: reenvía el lote de `once` (mismos eventId y versiones) --dup-k veces, mezclado.
	ProfileDup Profile = "dup"
	// ProfileOnce: cada medidor declara exactamente una vez.
	ProfileOnce Profile = "once"
)

// Vendor mixto por defecto: la mitad de los medidores de cada zona usa cada formato.
const VendorMixed = "mixed"

// Options son los parámetros de una corrida. Rate < 0 significa "sin fijar".
type Options struct {
	Profile  Profile
	Rate     float64       // declaraciones por segundo; 0 en once/dup = sin límite
	Duration time.Duration // steady, pairs, spike
	Meters   int
	Period   time.Duration // entre declaraciones de un mismo medidor (deriva --rate si falta)
	Zones    int
	Seed     int64
	DupK     int
	Out      string
	Prefix   string
	Vendor   string // mixed | acme-v1 | voltix-2 | cualquier otro valor: header desconocido con cuerpo acme
	Horizon  time.Duration
	Ramp     time.Duration
	Workers  int
	// VersionBase es la primera versión de cada medidor; 0 = 1 en once/dup y el unix time en el resto.
	VersionBase int64
	// LogPublish emite un log de etapa (stage=publish, eventId, traceId) por declaración confirmada: para
	// seguir un evento con `expctl trace`. Con cargas altas son demasiadas líneas: déjalo apagado.
	LogPublish bool
	Region     string
	AMQP       amqpx.Config
	OpsAddr    string
}

const (
	defaultRate = 100.0
	// QueueName y Exchange del canal de entrada.
	QueueName = "meter-declarations"
	Exchange  = "meter-declarations.x"
)

// secs es un flag que acepta segundos ("90", "0.5") o una duración Go ("1m30s").
type secs struct{ d *time.Duration }

func (s secs) String() string {
	if s.d == nil {
		return ""
	}
	return s.d.String()
}

func (s secs) Set(v string) error {
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		*s.d = time.Duration(f * float64(time.Second))
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%q is neither seconds nor a duration", v)
	}
	*s.d = d
	return nil
}

func parseDuration(v string) (time.Duration, error) {
	var d time.Duration
	err := secs{&d}.Set(v)
	return d, err
}

func durationEnv(r *envcfg.Reader, name string, def time.Duration) time.Duration {
	return envcfg.Parse(r, name, def, parseDuration)
}

const usage = `uso: meter-sim [flags]   (cada flag tiene su variable METERSIM_<NOMBRE>; el flag manda)

  --profile steady|pairs|spike|dup|once   forma de la carga (steady)
  --rate N            declaraciones/s (steady/pairs/spike: por defecto meters/period o 100; once/dup: 100; 0 = sin límite)
  --duration D        duración en s o 1m30s (steady, pairs, spike) (30s)
  --meters N          medidores (100)
  --period S          s entre declaraciones de un medidor; si falta --rate, rate = meters/period
  --zones N           zonas del mercado (4); debe coincidir con ZONES del gateway
  --seed N            semilla (1): fija sides, kWh, eventIds y el orden de dup
  --dup-k N           copias de cada declaración en dup (2)
  --out FILE          CSV eventId,meterId,version,sentAt (vacío = sin CSV)
  --prefix P          prefijo de los ids de medidor (m-): m-0001
  --vendor V          mixed (por defecto), acme-v1, voltix-2 u otro valor (header desconocido, cuerpo acme)
  --horizon S         vigencia de cada posición (120s)
  --ramp S            rampa de spike (5s)
  --workers N         publicadores concurrentes (16)
  --version-base N    primera versión de cada medidor (0 = 1 en once/dup, unix time en el resto)
  --log-publish       un log de etapa (eventId, traceId) por declaración publicada; para expctl trace con cargas pequeñas

Entorno: REGION (r1|r2), AMQP_HOST, AMQP_USER, AMQP_PASSWORD, AMQP_VHOST (por defecto REGION), OPS_ADDR (:9090).
`

// ParseOptions lee los flags (con las variables METERSIM_* como valores por defecto) y el entorno.
func ParseOptions(args []string, getenv func(string) string, stderr io.Writer) (Options, error) {
	r := envcfg.New(getenv)
	env := func(name string) string { return getenv("METERSIM_" + name) }
	pe := envcfg.New(env)

	o := Options{
		Profile:     Profile(pe.String("PROFILE", string(ProfileSteady))),
		Rate:        -1,
		Duration:    durationEnv(pe, "DURATION", 30*time.Second),
		Meters:      pe.Int("METERS", 100, 1),
		Period:      durationEnv(pe, "PERIOD", 0),
		Zones:       pe.Int("ZONES", 4, 1),
		Seed:        pe.Int64("SEED", 1),
		DupK:        pe.Int("DUP_K", 2, 1),
		Out:         pe.String("OUT", ""),
		Prefix:      pe.String("PREFIX", "m-"),
		Vendor:      pe.String("VENDOR", VendorMixed),
		Horizon:     durationEnv(pe, "HORIZON", 120*time.Second),
		Ramp:        durationEnv(pe, "RAMP", 5*time.Second),
		Workers:     pe.Int("WORKERS", 16, 1),
		VersionBase: pe.Int64("VERSION_BASE", 0),
	}
	o.Rate = pe.Float("RATE", -1, 0)
	o.LogPublish = pe.String("LOG_PUBLISH", "") == "1"

	fs := flag.NewFlagSet("meter-sim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { _, _ = fmt.Fprint(stderr, usage) }
	prof := string(o.Profile)
	fs.StringVar(&prof, "profile", prof, "")
	fs.Float64Var(&o.Rate, "rate", o.Rate, "")
	fs.Var(secs{&o.Duration}, "duration", "")
	fs.IntVar(&o.Meters, "meters", o.Meters, "")
	fs.Var(secs{&o.Period}, "period", "")
	fs.IntVar(&o.Zones, "zones", o.Zones, "")
	fs.Int64Var(&o.Seed, "seed", o.Seed, "")
	fs.IntVar(&o.DupK, "dup-k", o.DupK, "")
	fs.StringVar(&o.Out, "out", o.Out, "")
	fs.StringVar(&o.Prefix, "prefix", o.Prefix, "")
	fs.StringVar(&o.Vendor, "vendor", o.Vendor, "")
	fs.Var(secs{&o.Horizon}, "horizon", "")
	fs.Var(secs{&o.Ramp}, "ramp", "")
	fs.IntVar(&o.Workers, "workers", o.Workers, "")
	fs.Int64Var(&o.VersionBase, "version-base", o.VersionBase, "")
	fs.BoolVar(&o.LogPublish, "log-publish", o.LogPublish, "")
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	o.Profile = Profile(prof)

	o.Region = r.Region()
	o.OpsAddr = r.String("OPS_ADDR", ":9090")
	amqpCfg, amqpErr := amqpx.ConfigFromEnv(getenv)
	amqpCfg.ConnectionName = "meter-sim"
	o.AMQP = amqpCfg
	if err := envcfg.JoinErrs(pe.Err(), r.Err(), amqpErr, o.validate()); err != nil {
		return Options{}, err
	}
	o.resolve()
	return o, nil
}

// validate comprueba los parámetros independientes del perfil.
func (o *Options) validate() error {
	var errs []error
	switch o.Profile {
	case ProfileSteady, ProfilePairs, ProfileSpike, ProfileDup, ProfileOnce:
	default:
		errs = append(errs, fmt.Errorf("unknown profile %q (steady|pairs|spike|dup|once)", o.Profile))
	}
	if o.Meters < 1 || o.Zones < 1 || o.DupK < 1 || o.Workers < 1 {
		errs = append(errs, errors.New("--meters, --zones, --dup-k and --workers must be >= 1"))
	}
	if o.Horizon <= 0 || o.Horizon > 24*time.Hour {
		errs = append(errs, errors.New("--horizon must be between 0 and 24h"))
	}
	if o.Period < 0 || o.Ramp < 0 {
		errs = append(errs, errors.New("--period and --ramp must be >= 0"))
	}
	if strings.TrimSpace(o.Prefix) == "" || len(o.Prefix) > 40 {
		errs = append(errs, errors.New("--prefix must be 1-40 characters"))
	}
	if o.Vendor == "" {
		errs = append(errs, errors.New("--vendor must not be empty"))
	}
	if o.Profile == ProfilePairs && o.Meters < 2 {
		errs = append(errs, errors.New("pairs needs --meters >= 2"))
	}
	switch o.Profile {
	case ProfileSteady, ProfilePairs, ProfileSpike:
		if o.Duration <= 0 {
			errs = append(errs, fmt.Errorf("profile %s needs --duration > 0", o.Profile))
		}
		if o.Rate == 0 {
			errs = append(errs, fmt.Errorf("profile %s needs --rate > 0 (or --period)", o.Profile))
		}
		if o.Rate < 0 && o.Period < 0 {
			errs = append(errs, errors.New("invalid --period"))
		}
	}
	if o.Rate < 0 && o.Rate != -1 {
		errs = append(errs, errors.New("--rate must be >= 0"))
	}
	return errors.Join(errs...)
}

// resolve rellena los valores derivados: tasa por defecto y versión inicial.
func (o *Options) resolve() {
	if o.Rate < 0 {
		switch {
		case (o.Profile == ProfileSteady || o.Profile == ProfilePairs || o.Profile == ProfileSpike) && o.Period > 0:
			o.Rate = float64(o.Meters) / o.Period.Seconds()
		default:
			o.Rate = defaultRate
		}
	}
	if o.VersionBase == 0 {
		if o.Profile == ProfileOnce || o.Profile == ProfileDup {
			o.VersionBase = 1
		} else {
			o.VersionBase = time.Now().Unix()
		}
	}
}

// IsKnownVendor indica si v es uno de los formatos que entiende la ACL.
func IsKnownVendor(v string) bool { return v == acl.VendorAcme || v == acl.VendorVoltix }
