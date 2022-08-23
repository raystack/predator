package query_test

import (
	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFromClause(t *testing.T) {
	t.Run("Unnest", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return unnest expression given correct values", func(t *testing.T) {
				unnest := query.Unnest{
					ColumnName: "status",
					Alias:      "unnest1",
				}
				expected := "UNNEST(status) as unnest1"

				unnestStr := unnest.Build()
				assert.Equal(t, expected, unnestStr)
			})
		})
	})
	t.Run("FromClause", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return from clause given correct unnest items and table information", func(t *testing.T) {
				unnestClauses := []*query.Unnest{
					{
						ColumnName: "abc",
						Alias:      "unnest1",
					},
				}

				fromClause := query.FromClause{
					TableID:       "project.dataset.table",
					UnnestClauses: unnestClauses,
				}

				expected := "`project.dataset.table` , UNNEST(abc) as unnest1"

				clauses := fromClause.Build()
				assert.Equal(t, expected, clauses)
			})

			t.Run("should return from clause given no unnest items and table information", func(t *testing.T) {

				fromClause := query.FromClause{
					TableID: "project.dataset.table",
				}

				expected := "`project.dataset.table`"

				clauses := fromClause.Build()
				assert.Equal(t, expected, clauses)
			})
		})
	})
}
