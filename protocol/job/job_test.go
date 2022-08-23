package job

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDiffBetween(t *testing.T) {
	t.Run("DiffBetween", func(t *testing.T) {
		t.Run("should calculate diff", func(t *testing.T) {
			sourceURNs := []string{
				"sample-project1.dataset_a.table_x",
				"sample-project2.dataset_b.table_x",
			}

			destURNs := []string{
				"sample-project1.dataset_a.table_x",
				"sample-project1.dataset_c.table_x",
				"sample-project1.dataset_d.table_x",
			}

			expected := &Diff{
				Add:    []string{"sample-project2.dataset_b.table_x"},
				Remove: []string{"sample-project1.dataset_c.table_x", "sample-project1.dataset_d.table_x"},
				Update: []string{"sample-project1.dataset_a.table_x"},
			}

			diff := DiffBetween(sourceURNs, destURNs)

			assert.Equal(t, 1, diff.AddedCount())
			assert.Equal(t, 2, diff.RemovedCount())
			assert.Equal(t, 1, diff.UpdatedCount())
			assert.Equal(t, expected, diff)
		})
	})
}
