package matching

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"go.yaml.in/yaml/v3"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// rulesFile es el YAML del ConfigMap matching-rules (rules.yaml).
//
//	params:            # constantes que ven ambas expresiones como "params"
//	  minKwh: 0.1
//	eligible: "position.kwh >= params.minKwh"           # por posición (bool)
//	pair: "seller.zone == buyer.zone"                    # por par (bool)
type rulesFile struct {
	Params   map[string]any `yaml:"params"`
	Eligible string         `yaml:"eligible"`
	Pair     string         `yaml:"pair"`
}

// eligibleEnv es el entorno de la expresión eligible.
type eligibleEnv struct {
	Position Position       `expr:"position"`
	Price    Price          `expr:"price"`
	Params   map[string]any `expr:"params"`
}

// pairEnv es el entorno de la expresión pair.
type pairEnv struct {
	Seller Position       `expr:"seller"`
	Buyer  Position       `expr:"buyer"`
	Price  Price          `expr:"price"`
	Params map[string]any `expr:"params"`
}

// Rules son las reglas compiladas. Es inmutable: una recarga crea otro valor.
type Rules struct {
	// Version es el prefijo del sha256 del archivo (identifica la versión cargada).
	Version string
	Params  map[string]any
	// Source son las expresiones originales (para /debug/state).
	EligibleSrc string
	PairSrc     string

	eligible *vm.Program
	pair     *vm.Program
}

// ParseRules parsea y compila el YAML. Cualquier error (YAML, campos desconocidos,
// expresión que no compila o no devuelve bool) lo rechaza entero.
func ParseRules(data []byte) (*Rules, error) {
	var f rulesFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("rules: parse yaml: %w", err)
	}
	if strings.TrimSpace(f.Eligible) == "" || strings.TrimSpace(f.Pair) == "" {
		return nil, errors.New("rules: eligible and pair are required")
	}
	if f.Params == nil {
		f.Params = map[string]any{}
	}
	eligible, err := expr.Compile(f.Eligible, expr.Env(eligibleEnv{}), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("rules: compile eligible: %w", err)
	}
	pair, err := expr.Compile(f.Pair, expr.Env(pairEnv{}), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("rules: compile pair: %w", err)
	}
	sum := sha256.Sum256(data)
	return &Rules{Version: hex.EncodeToString(sum[:])[:12], Params: f.Params,
		EligibleSrc: f.Eligible, PairSrc: f.Pair, eligible: eligible, pair: pair}, nil
}

// Eligible evalúa la regla eligible para una posición.
func (r *Rules) Eligible(p Position, price Price) (bool, error) {
	return runBool(r.eligible, eligibleEnv{Position: p, Price: price, Params: r.Params})
}

// CanPair evalúa la regla pair para un oferente y un demandante.
func (r *Rules) CanPair(seller, buyer Position, price Price) (bool, error) {
	return runBool(r.pair, pairEnv{Seller: seller, Buyer: buyer, Price: price, Params: r.Params})
}

func runBool(p *vm.Program, env any) (bool, error) {
	out, err := expr.Run(p, env)
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("rules: expression returned %T, want bool", out)
	}
	return b, nil
}
