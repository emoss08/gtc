package meilisearch

import "github.com/emoss08/gtc/internal/infrastructure/config"

type IndexResolver interface {
	GetIndex(schema, table string) (string, bool)
	GetPrimaryKey(schema, table string) string
	IndexSettings() (map[string]config.MeiliIndexSettings, error)
}

type TableMapper struct {
	resolver IndexResolver
}

type TableMapperParams struct {
	Resolver IndexResolver
}

func NewTableMapper(p TableMapperParams) *TableMapper {
	return &TableMapper{resolver: p.Resolver}
}

func (m *TableMapper) GetIndex(schema, table string) (string, bool) {
	return m.resolver.GetIndex(schema, table)
}

func (m *TableMapper) ShouldProcess(schema, table string) bool {
	_, exists := m.GetIndex(schema, table)
	return exists
}

// PrimaryKey is the document key field for the table's index.
func (m *TableMapper) PrimaryKey(schema, table string) string {
	return m.resolver.GetPrimaryKey(schema, table)
}

// IndexSettings returns the settings to apply per index at startup.
func (m *TableMapper) IndexSettings() (map[string]config.MeiliIndexSettings, error) {
	return m.resolver.IndexSettings()
}
