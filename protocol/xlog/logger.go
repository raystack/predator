package xlog

import (
	"fmt"
	"log"
	"os"
	"strings"
)

//Value is structured key=value information added to logging
type Value struct {
	Key   string
	Value interface{}
}

//NewValue create logger value
func NewValue(key string, value interface{}) Value {
	return Value{
		Key:   key,
		Value: value,
	}
}

// key=value, key=value
func serialise(values []Value) string {
	var oneLines []string
	for _, v := range values {
		r := fmt.Sprintf("%v=%v", v.Key, v.Value)
		oneLines = append(oneLines, r)
	}
	if len(oneLines) > 0 {
		return strings.Join(oneLines, " ,")
	}

	return ""
}

var defaultLogger = log.New(os.Stdout, "INFO: ", log.LstdFlags)

func Format(message string, values ...Value) string {
	info := serialise(values)
	return strings.Join([]string{info, message}, " ,")
}

//Info print logging
func Info(message string, values ...Value) {
	info := serialise(values)
	defaultLogger.Println(info, message)
}
