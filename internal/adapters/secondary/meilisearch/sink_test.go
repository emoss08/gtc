package meilisearch

import (
	"errors"
	"net/http"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/meilisearch/meilisearch-go"
)

func TestClassifyAPIError(t *testing.T) {
	type kind int
	const (
		transient kind = iota
		permanent
		unavailable
	)

	for _, tc := range []struct {
		name string
		err  error
		want kind
	}{
		{"connection refused", &meilisearch.Error{ErrCode: meilisearch.MeilisearchCommunicationError}, unavailable},
		{"client timeout", &meilisearch.Error{ErrCode: meilisearch.MeilisearchTimeoutError}, unavailable},
		{"client retries spent", &meilisearch.Error{ErrCode: meilisearch.MeilisearchMaxRetriesExceeded}, unavailable},
		{"503", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusServiceUnavailable}, unavailable},
		{"500", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusInternalServerError}, unavailable},
		{"429", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusTooManyRequests}, unavailable},
		{"401", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusUnauthorized}, unavailable},
		{"400 bad document", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusBadRequest}, permanent},
		{"413 payload too large", &meilisearch.Error{ErrCode: meilisearch.MeilisearchApiError, StatusCode: http.StatusRequestEntityTooLarge}, permanent},
		{"unencodable payload", &meilisearch.Error{ErrCode: meilisearch.ErrCodeMarshalRequest}, permanent},
		{"unreadable response", &meilisearch.Error{ErrCode: meilisearch.ErrCodeResponseUnmarshalBody, StatusCode: http.StatusOK}, transient},
		{"not a client error", errors.New("boom"), transient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyAPIError(tc.err)
			got := transient
			switch {
			case domain.IsPermanent(err):
				got = permanent
			case domain.IsUnavailable(err):
				got = unavailable
			}
			if got != tc.want {
				t.Errorf("classified %v as %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}
