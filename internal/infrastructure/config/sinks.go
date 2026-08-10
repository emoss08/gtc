package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SinksConfig struct {
	RedisStream RedisKeyedSinkConfig  `yaml:"redis_stream"`
	RedisJSON   RedisKeyedSinkConfig  `yaml:"redis_json"`
	Meilisearch MeilisearchSinkConfig `yaml:"meilisearch"`
	Outbox      OutboxConfig          `yaml:"outbox"`
}

// OutboxConfig enables the transactional-outbox sink: INSERTs into the
// configured table are published as domain events to a Redis stream named by
// the row's topic column, instead of the table being mirrored like data.
type OutboxConfig struct {
	// Table is the outbox table ("schema.table"; bare names default to the
	// public schema). Setting it enables the outbox sink.
	Table string `yaml:"table"`
	// StreamPrefix prefixes destination streams: <prefix>:<topic>.
	StreamPrefix string `yaml:"stream_prefix"`
	// DefaultTopic is used when a row's topic column is null or empty.
	// Without it, such rows are an error (visible, not silently dropped).
	DefaultTopic string `yaml:"default_topic"`
	// DeleteAfterPublish deletes each outbox row after successful publish,
	// keeping the table small without a cleanup job.
	DeleteAfterPublish bool          `yaml:"delete_after_publish"`
	Columns            OutboxColumns `yaml:"columns"`
}

type OutboxColumns struct {
	ID        string `yaml:"id"`
	Topic     string `yaml:"topic"`
	Key       string `yaml:"key"`
	Payload   string `yaml:"payload"`
	EventType string `yaml:"event_type"`
}

func (c *OutboxConfig) Enabled() bool { return c.Table != "" }

func (c *OutboxConfig) applyDefaults() {
	if c.StreamPrefix == "" {
		c.StreamPrefix = "events"
	}
	if c.Columns.ID == "" {
		c.Columns.ID = "id"
	}
	if c.Columns.Topic == "" {
		c.Columns.Topic = "topic"
	}
	if c.Columns.Key == "" {
		c.Columns.Key = "aggregate_id"
	}
	if c.Columns.Payload == "" {
		c.Columns.Payload = "payload"
	}
	if c.Columns.EventType == "" {
		c.Columns.EventType = "event_type"
	}
}

// SchemaTable splits the configured table into schema and table, defaulting
// the schema to "public".
func (c *OutboxConfig) SchemaTable() (string, string) {
	for i := 0; i < len(c.Table); i++ {
		if c.Table[i] == '.' {
			return c.Table[:i], c.Table[i+1:]
		}
	}
	return "public", c.Table
}

// TransformSpec declares per-event transformations applied before a sink
// processes an event: a CEL row filter, column removal, and column masking.
type TransformSpec struct {
	// Filter is a CEL expression; the event is processed only when it
	// evaluates to true. Variables: op, schema, table, new, old.
	Filter string `yaml:"filter"`
	// DropColumns are removed from the event's data before it reaches the
	// sink (e.g. password_hash).
	DropColumns []string `yaml:"drop_columns"`
	// Mask maps column names to a masking strategy: redact, null, sha256,
	// or last4.
	Mask map[string]string `yaml:"mask"`
}

func (t TransformSpec) IsZero() bool {
	return t.Filter == "" && len(t.DropColumns) == 0 && len(t.Mask) == 0
}

// RedisTableConfig is the per-table entry for a Redis-backed sink. In YAML it
// is either a plain string (the key pattern, kept for backward compatibility)
// or an object with a key pattern and transforms.
type RedisTableConfig struct {
	Key           string `yaml:"key"`
	TransformSpec `yaml:",inline"`
}

func (c *RedisTableConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&c.Key)
	}
	type raw RedisTableConfig
	return node.Decode((*raw)(c))
}

// RedisKeyedSinkConfig configures a Redis-backed sink whose keys are built
// from a per-table key pattern. It is shared by the stream and JSON sinks.
type RedisKeyedSinkConfig struct {
	SyncAll           bool                        `yaml:"sync_all"`
	DefaultKeyPattern string                      `yaml:"default_key_pattern"`
	Transform         *TransformSpec              `yaml:"transform"`
	Tables            map[string]RedisTableConfig `yaml:"tables"`
}

func (c *RedisKeyedSinkConfig) ShouldProcess(schema, table string) bool {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if _, ok := c.Tables[fullName]; ok {
		return true
	}
	if _, ok := c.Tables[table]; ok {
		return true
	}

	return c.SyncAll
}

// GetKeyPattern returns the configured pattern for the table, or "" when
// nothing is configured; the sink's key resolver applies its own default.
func (c *RedisKeyedSinkConfig) GetKeyPattern(schema, table string) string {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if tc, ok := c.Tables[fullName]; ok && tc.Key != "" {
		return tc.Key
	}

	if tc, ok := c.Tables[table]; ok && tc.Key != "" {
		return tc.Key
	}

	return c.DefaultKeyPattern
}

// TableTransforms returns the per-table transform specs keyed by the table
// name as written in the config.
func (c *RedisKeyedSinkConfig) TableTransforms() map[string]TransformSpec {
	out := make(map[string]TransformSpec)
	for name, tc := range c.Tables {
		if !tc.TransformSpec.IsZero() {
			out[name] = tc.TransformSpec
		}
	}
	return out
}

// MeiliTableConfig is the per-table entry for the Meilisearch sink: either a
// plain string (the index name) or an object with an index and transforms.
type MeiliTableConfig struct {
	Index         string `yaml:"index"`
	TransformSpec `yaml:",inline"`
}

func (c *MeiliTableConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&c.Index)
	}
	type raw MeiliTableConfig
	return node.Decode((*raw)(c))
}

type MeilisearchSinkConfig struct {
	Transform *TransformSpec              `yaml:"transform"`
	Tables    map[string]MeiliTableConfig `yaml:"tables"`
}

func (c *MeilisearchSinkConfig) GetIndex(schema, table string) (string, bool) {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if tc, ok := c.Tables[fullName]; ok {
		return tc.Index, tc.Index != ""
	}

	if tc, ok := c.Tables[table]; ok {
		return tc.Index, tc.Index != ""
	}

	return "", false
}

func (c *MeilisearchSinkConfig) TableTransforms() map[string]TransformSpec {
	out := make(map[string]TransformSpec)
	for name, tc := range c.Tables {
		if !tc.TransformSpec.IsZero() {
			out[name] = tc.TransformSpec
		}
	}
	return out
}

func LoadSinksConfig() (*SinksConfig, error) {
	cfg := &SinksConfig{}

	configPath := os.Getenv("SINK_CONFIG_FILE")
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read sinks config file: %w", err)
		}
		if unmarshalErr := yaml.Unmarshal(data, cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("parse sinks config: %w", unmarshalErr)
		}
	}

	if cfg.RedisStream.Tables == nil {
		cfg.RedisStream.Tables = make(map[string]RedisTableConfig)
	}
	if cfg.RedisJSON.Tables == nil {
		cfg.RedisJSON.Tables = make(map[string]RedisTableConfig)
	}
	if cfg.Meilisearch.Tables == nil {
		cfg.Meilisearch.Tables = make(map[string]MeiliTableConfig)
	}

	// Plug-and-play default: with no sink config file, mirror every table.
	if configPath == "" {
		cfg.RedisStream.SyncAll = true
		cfg.RedisJSON.SyncAll = true
	}

	if cfg.Outbox.Enabled() {
		cfg.Outbox.applyDefaults()
	}

	return cfg, nil
}
