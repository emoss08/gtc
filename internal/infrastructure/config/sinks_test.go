package config

import (
	"slices"
	"strings"
	"testing"
)

func TestMeiliTableConfigParsesIndexSettings(t *testing.T) {
	var cfg SinksConfig
	err := yamlUnmarshal(t, `
meilisearch:
  tables:
    public.products: products
    public.shipments:
      index: shipments
      primary_key: id
      searchable_attributes: [pro_number, bol]
      filterable_attributes: [organization_id, business_unit_id]
`, &cfg)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := cfg.Meilisearch.GetPrimaryKey("public", "shipments"); got != "id" {
		t.Errorf("shipments primary key = %q, want id", got)
	}
	if got := cfg.Meilisearch.GetPrimaryKey("public", "products"); got != DefaultMeiliPrimaryKey {
		t.Errorf("a table without primary_key must default to %q, got %q", DefaultMeiliPrimaryKey, got)
	}

	settings, err := cfg.Meilisearch.IndexSettings()
	if err != nil {
		t.Fatalf("IndexSettings: %v", err)
	}
	shipments := settings["shipments"]
	if !slices.Equal(shipments.SearchableAttributes, []string{"pro_number", "bol"}) {
		t.Errorf("searchable attributes = %v", shipments.SearchableAttributes)
	}
	if !slices.Equal(shipments.FilterableAttributes, []string{"organization_id", "business_unit_id"}) {
		t.Errorf("filterable attributes = %v", shipments.FilterableAttributes)
	}
	if settings["products"].PrimaryKey != DefaultMeiliPrimaryKey {
		t.Errorf("plain-string entries must get the default primary key, got %q", settings["products"].PrimaryKey)
	}
}

func TestIndexSettingsRejectsConflictingTablesForOneIndex(t *testing.T) {
	cfg := MeilisearchSinkConfig{Tables: map[string]MeiliTableConfig{
		"public.a": {Index: "shared", SearchableAttributes: []string{"name"}},
		"public.b": {Index: "shared", SearchableAttributes: []string{"title"}},
	}}

	_, err := cfg.IndexSettings()
	if err == nil || !strings.Contains(err.Error(), `"shared"`) {
		t.Fatalf("want an error naming the shared index, got %v", err)
	}
}

func TestIndexSettingsAllowsAgreeingTablesForOneIndex(t *testing.T) {
	cfg := MeilisearchSinkConfig{Tables: map[string]MeiliTableConfig{
		"public.a": {Index: "shared", PrimaryKey: "id", SearchableAttributes: []string{"name"}},
		"public.b": {Index: "shared", SearchableAttributes: []string{"name"}},
	}}

	settings, err := cfg.IndexSettings()
	if err != nil {
		t.Fatalf("tables agreeing on settings must be accepted: %v", err)
	}
	if len(settings) != 1 {
		t.Errorf("want one index, got %d", len(settings))
	}
}
