package util

import (
	"fmt"
	"github.com/google/uuid"
)

func IsUUIDValid(ID string) bool {
	_, err := uuid.Parse(ID)
	return err == nil
}

var zeroFloat = 0.0000000

func RoundMetricValue(value float64) string {
	format := "%.3F"
	rounded := fmt.Sprintf(format, value)
	if rounded == "0.000" {
		if value > zeroFloat {
			rounded = "0.001"
		} else if value < zeroFloat {
			rounded = "-0.001"
		} else {
			rounded = "0.000"
		}
	}
	return rounded
}

func DoubleQuote(content string) string {
	return fmt.Sprintf("\"%s\"", content)
}
