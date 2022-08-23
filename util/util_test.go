package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoundMetricValue(t *testing.T) {
	t.Run("RoundMetricValue", func(t *testing.T) {
		t.Run("fraction with than less than or equal to 3 shown as the 3 digit fraction", func(t *testing.T) {
			result1 := RoundMetricValue(0.19)
			assert.Equal(t, "0.190", result1)

			result2 := RoundMetricValue(0.012)
			assert.Equal(t, "0.012", result2)

			result3 := RoundMetricValue(0.00498982)
			assert.Equal(t, "0.005", result3)

		})
		t.Run("fraction with more than 3 digit fraction rounded to 3 digit", func(t *testing.T) {
			result := RoundMetricValue(0.0000000008)
			assert.Equal(t, "0.001", result)
		})
		t.Run("actual zero should stays zero", func(t *testing.T) {
			result := RoundMetricValue(0.0000000000)
			assert.Equal(t, "0.000", result)
		})
	})
}
