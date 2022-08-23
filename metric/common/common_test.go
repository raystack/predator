package common

import (
	"cloud.google.com/go/civil"
	"encoding/base64"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConvertValueToString(t *testing.T) {
	t.Run("should return supported values", func(t *testing.T) {
		bytesValue := []byte{1, 2, 3}
		timeValue := civil.Time{
			Hour:       10,
			Minute:     0,
			Second:     0,
			Nanosecond: 0,
		}
		dateValue := civil.Date{
			Year:  2012,
			Month: 1,
			Day:   1,
		}
		dateTimeValue := civil.DateTime{
			Date: dateValue,
			Time: timeValue,
		}
		values := []interface{}{
			1,
			0.123456789,
			true,
			"success",
			bytesValue,
			dateValue,
			timeValue,
			dateTimeValue,
			time.Date(2012, 1, 1, 10, 0, 0, 0, time.UTC),
		}

		var result []string
		for _, v := range values {
			r, _ := ConvertValueToString(v)
			result = append(result, r)
		}

		expected := []string{
			"1",
			"0.123456789",
			"true",
			"success",
			base64.StdEncoding.EncodeToString(bytesValue),
			"2012-01-01",
			"10:00:00",
			"2012-01-01 10:00:00",
			"1325412000000",
		}

		for i, r := range result {
			assert.Equal(t, expected[i], r)
		}
	})
	t.Run("should return error for unsupported value", func(t *testing.T) {
		var unsupported *metric.Spec
		_, err := ConvertValueToString(unsupported)
		assert.Error(t, err)

		var unsupportedSlice []int
		_, err = ConvertValueToString(unsupportedSlice)
		assert.Error(t, err)
	})
}
