package server

import (
	"encoding/json"
	"net/http"

	"github.com/emoss08/gtc/internal/core/domain"
)

type schemaChangeResponse struct {
	domain.SchemaChange
	// Breaking and Summary are derived; the dashboard renders them directly
	// instead of reimplementing the rules.
	Breaking bool   `json:"breaking"`
	Summary  string `json:"summary"`
}

// handleSchema returns recently detected DDL changes, newest first.
func (s *Server) handleSchema(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	changes := []schemaChangeResponse{}
	if s.schema != nil {
		for _, change := range s.schema.History() {
			changes = append(changes, schemaChangeResponse{
				SchemaChange: change,
				Breaking:     change.Breaking(),
				Summary:      change.Summary(),
			})
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"total":   len(changes),
		"changes": changes,
	})
}
