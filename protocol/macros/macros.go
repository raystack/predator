package macros

import (
	"errors"
	"regexp"
)

//Type is macro type
type Type string

//Partition is macros that contains partition expression query
const Partition Type = "__PARTITION__"

var partitionMacrosRg = regexp.MustCompile(string(Partition))

//IsUsingMacros to check if a text contains macros
func IsUsingMacros(text string, macros Type) bool {
	var reg *regexp.Regexp

	switch macros {
	case Partition:
		reg = partitionMacrosRg
	default:
		return false
	}
	return reg.MatchString(text)
}

//ReplaceMacros to replace macros in a text
func ReplaceMacros(text string, renderedValue string, macros Type) (string, error) {
	var reg *regexp.Regexp

	switch macros {
	case Partition:
		reg = partitionMacrosRg
	default:
		return "", errors.New("unsupported macros")
	}

	newTex := reg.ReplaceAllString(text, renderedValue)
	return newTex, nil
}
