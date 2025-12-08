package meilisearch

type TableMapper struct {
	mapping map[string]string
}

func NewTableMapper(mapping map[string]string) *TableMapper {
	return &TableMapper{mapping: mapping}
}

func (m *TableMapper) GetIndex(schema, table string) (string, bool) {
	fullName := schema + "." + table
	index, exists := m.mapping[fullName]
	return index, exists
}

func (m *TableMapper) ShouldProcess(schema, table string) bool {
	_, exists := m.GetIndex(schema, table)
	return exists
}
