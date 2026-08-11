package transform

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

// celEnv declares the variables available to filter expressions.
func celEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("op", cel.StringType),
		cel.Variable("schema", cel.StringType),
		cel.Variable("table", cel.StringType),
		cel.Variable("new", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("old", cel.MapType(cel.StringType, cel.DynType)),
	)
}

type maskFunc func(any) any

var maskStrategies = map[string]maskFunc{
	"redact": func(any) any { return "[REDACTED]" },
	"null":   func(any) any { return nil },
	"sha256": func(v any) any {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%v", v)))
		return hex.EncodeToString(sum[:])
	},
	"last4": func(v any) any {
		runes := []rune(fmt.Sprintf("%v", v))
		if len(runes) <= 4 {
			return "****"
		}
		return strings.Repeat("*", len(runes)-4) + string(runes[len(runes)-4:])
	},
}

// Options carries deployment-level inputs that mask strategies need.
type Options struct {
	// HMACKey keys the hmac256 strategy (CDC_MASK_HMAC_KEY). Unlike plain
	// sha256, values masked with a keyed hash cannot be recovered by hashing
	// candidate inputs (dictionary attack) without the key, while staying
	// deterministic so masked columns remain joinable across events.
	HMACKey []byte
}

// strategyFunc resolves a mask strategy name, including the keyed hmac256
// strategy which is only available when a key is configured.
func strategyFunc(name string, opts Options) (maskFunc, error) {
	if fn, ok := maskStrategies[name]; ok {
		return fn, nil
	}
	if name == "hmac256" {
		if len(opts.HMACKey) == 0 {
			return nil, fmt.Errorf("mask strategy hmac256 requires CDC_MASK_HMAC_KEY to be set")
		}
		key := opts.HMACKey
		return func(v any) any {
			mac := hmac.New(sha256.New, key)
			mac.Write([]byte(fmt.Sprintf("%v", v)))
			return hex.EncodeToString(mac.Sum(nil))
		}, nil
	}
	return nil, fmt.Errorf(
		"unknown mask strategy %q (valid: redact, null, sha256, hmac256, last4)", name)
}

// Chain is a compiled sequence of transform specs applied to an event in
// order. Compilation happens once at startup so bad expressions fail fast.
type Chain struct {
	steps []compiledSpec
}

type compiledSpec struct {
	filter cel.Program
	drop   map[string]struct{}
	mask   map[string]maskFunc
}

// Compile validates and compiles transform specs into a Chain. A nil Chain
// (no specs) is valid and applies nothing.
func Compile(specs ...config.TransformSpec) (*Chain, error) {
	return CompileWithOptions(Options{}, specs...)
}

// CompileWithOptions is Compile with deployment-level options (e.g. the
// hmac256 key). Compilation happens once at startup so a strategy whose
// prerequisites are missing fails fast instead of at event time.
func CompileWithOptions(opts Options, specs ...config.TransformSpec) (*Chain, error) {
	var steps []compiledSpec

	var env *cel.Env
	for _, spec := range specs {
		if spec.IsZero() {
			continue
		}

		step := compiledSpec{}

		if spec.Filter != "" {
			if env == nil {
				var err error
				if env, err = celEnv(); err != nil {
					return nil, fmt.Errorf("create CEL environment: %w", err)
				}
			}
			ast, issues := env.Compile(spec.Filter)
			if issues != nil && issues.Err() != nil {
				return nil, fmt.Errorf("compile filter %q: %w", spec.Filter, issues.Err())
			}
			if ast.OutputType() != cel.BoolType {
				return nil, fmt.Errorf("filter %q must evaluate to a boolean, got %s",
					spec.Filter, ast.OutputType())
			}
			prg, err := env.Program(ast)
			if err != nil {
				return nil, fmt.Errorf("build filter program %q: %w", spec.Filter, err)
			}
			step.filter = prg
		}

		if len(spec.DropColumns) > 0 {
			step.drop = make(map[string]struct{}, len(spec.DropColumns))
			for _, col := range spec.DropColumns {
				step.drop[col] = struct{}{}
			}
		}

		if len(spec.Mask) > 0 {
			step.mask = make(map[string]maskFunc, len(spec.Mask))
			for col, strategy := range spec.Mask {
				fn, err := strategyFunc(strategy, opts)
				if err != nil {
					return nil, fmt.Errorf("mask for column %q: %w", col, err)
				}
				step.mask[col] = fn
			}
		}

		steps = append(steps, step)
	}

	if len(steps) == 0 {
		return nil, nil
	}
	return &Chain{steps: steps}, nil
}

// Apply runs the chain against an event. It returns the (possibly rewritten)
// event and whether it should be processed; the input event's data maps are
// never mutated, since they are shared with other sinks.
func (c *Chain) Apply(event domain.CDCEvent) (domain.CDCEvent, bool, error) {
	if c == nil {
		return event, true, nil
	}

	cloned := false
	for _, step := range c.steps {
		if step.filter != nil {
			keep, err := evalFilter(step.filter, event)
			if err != nil {
				return event, false, err
			}
			if !keep {
				return event, false, nil
			}
		}

		if len(step.drop) == 0 && len(step.mask) == 0 {
			continue
		}
		if !cloned {
			event.NewData = cloneMap(event.NewData)
			event.OldData = cloneMap(event.OldData)
			cloned = true
		}

		for col := range step.drop {
			delete(event.NewData, col)
			delete(event.OldData, col)
		}
		for col, fn := range step.mask {
			maskColumn(event.NewData, col, fn)
			maskColumn(event.OldData, col, fn)
		}
	}

	return event, true, nil
}

func evalFilter(prg cel.Program, event domain.CDCEvent) (bool, error) {
	newData := event.NewData
	if newData == nil {
		newData = map[string]any{}
	}
	oldData := event.OldData
	if oldData == nil {
		oldData = map[string]any{}
	}

	out, _, err := prg.Eval(map[string]any{
		"op":     event.Operation.String(),
		"schema": event.Schema,
		"table":  event.Table,
		"new":    newData,
		"old":    oldData,
	})
	if err != nil {
		return false, fmt.Errorf("evaluate filter for %s.%s: %w", event.Schema, event.Table, err)
	}

	keep, ok := out.(types.Bool)
	if !ok {
		return false, fmt.Errorf("filter for %s.%s returned %T, expected bool",
			event.Schema, event.Table, out)
	}
	return bool(keep), nil
}

func maskColumn(data map[string]any, col string, fn maskFunc) {
	val, ok := data[col]
	if !ok || val == nil {
		return
	}
	data[col] = fn(val)
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
