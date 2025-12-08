package meilisearch

type IndexResolver interface {
	GetIndex(schema, table string) (string, bool)
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
