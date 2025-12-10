package redis

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/emoss08/gtc/internal/core/domain"
)

const (
	DefaultStreamKeyPattern = "{{.Prefix}}:{{.Schema}}:{{.Table}}"
	DefaultJSONKeyPattern   = "{{.Prefix}}:{{.Schema}}:{{.Table}}:{{.Field \"id\"}}"
)

type KeyTemplate struct {
	pattern  string
	template *template.Template
}

type KeyTemplateContext struct {
	Prefix    string
	Schema    string
	Table     string
	Operation string
	ID        string
	newData   map[string]any
	oldData   map[string]any
}

func NewKeyTemplateContext(prefix string, event domain.CDCEvent) KeyTemplateContext {
	return KeyTemplateContext{
		Prefix:    prefix,
		Schema:    event.Schema,
		Table:     event.Table,
		Operation: event.Operation.String(),
		ID:        event.ID,
		newData:   event.NewData,
		oldData:   event.OldData,
	}
}

func (c KeyTemplateContext) Field(name string) string {
	if c.newData != nil {
		if val, ok := c.newData[name]; ok {
			return formatFieldValue(val)
		}
	}

	if c.oldData != nil {
		if val, ok := c.oldData[name]; ok {
			return formatFieldValue(val)
		}
	}

	return ""
}

func formatFieldValue(val any) string {
	if val == nil {
		return "null"
	}
	return fmt.Sprintf("%v", val)
}

func ParseKeyTemplate(pattern string) (*KeyTemplate, error) {
	tmpl, err := template.New("key").Parse(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid key pattern %q: %w", pattern, err)
	}

	return &KeyTemplate{
		pattern:  pattern,
		template: tmpl,
	}, nil
}

func MustParseKeyTemplate(pattern string) *KeyTemplate {
	kt, err := ParseKeyTemplate(pattern)
	if err != nil {
		panic(err)
	}
	return kt
}

func (kt *KeyTemplate) Execute(ctx KeyTemplateContext) (string, error) {
	var buf bytes.Buffer
	if err := kt.template.Execute(&buf, ctx); err != nil {
		return "", fmt.Errorf("failed to execute key template: %w", err)
	}
	return buf.String(), nil
}

func (kt *KeyTemplate) Pattern() string {
	return kt.pattern
}

type PatternResolver interface {
	GetKeyPattern(schema, table string) string
}

type TableFilter interface {
	ShouldProcess(schema, table string) bool
}

type KeyResolver struct {
	resolver       PatternResolver
	filter         TableFilter
	prefixTemplate *KeyTemplate
	templateCache  map[string]*KeyTemplate
}

type KeyResolverParams struct {
	Resolver PatternResolver
	Filter   TableFilter
	Prefix   string
}

func NewKeyResolver(p KeyResolverParams) (*KeyResolver, error) {
	prefixTmpl, err := ParseKeyTemplate(p.Prefix)
	if err != nil {
		return nil, fmt.Errorf("invalid prefix template: %w", err)
	}

	return &KeyResolver{
		resolver:       p.Resolver,
		filter:         p.Filter,
		prefixTemplate: prefixTmpl,
		templateCache:  make(map[string]*KeyTemplate),
	}, nil
}

func (kr *KeyResolver) ShouldProcess(schema, table string) bool {
	if kr.filter == nil {
		return true
	}
	return kr.filter.ShouldProcess(schema, table)
}

func (kr *KeyResolver) GenerateKey(event domain.CDCEvent) (string, error) {
	prefixCtx := KeyTemplateContext{
		Schema:    event.Schema,
		Table:     event.Table,
		Operation: event.Operation.String(),
		ID:        event.ID,
		newData:   event.NewData,
		oldData:   event.OldData,
	}
	prefix, err := kr.prefixTemplate.Execute(prefixCtx)
	if err != nil {
		return "", fmt.Errorf("failed to resolve prefix: %w", err)
	}

	pattern := kr.resolver.GetKeyPattern(event.Schema, event.Table)

	tmpl, ok := kr.templateCache[pattern]
	if !ok {
		tmpl, err = ParseKeyTemplate(pattern)
		if err != nil {
			return "", fmt.Errorf("invalid pattern for %s.%s: %w", event.Schema, event.Table, err)
		}
		kr.templateCache[pattern] = tmpl
	}

	ctx := NewKeyTemplateContext(prefix, event)
	return tmpl.Execute(ctx)
}
