package query

import (
	"fmt"
	"strings"
)

//Unnest is a definition clause for unnesting array columns, to become a normal columns
type Unnest struct {
	ColumnName string
	Alias      string
}

//Build is method to build unnest string
func (u *Unnest) Build() string {
	return fmt.Sprintf("UNNEST(%s) as %s", u.ColumnName, u.Alias)
}

//Equal comparable
func (u *Unnest) Equal(other *Unnest) bool {
	return *u == *other
}

//FromClause is a definition of from clause in sql
type FromClause struct {
	TableID       string
	UnnestClauses []*Unnest
}

//Build is a method to build from clause sql string
func (fc *FromClause) Build() string {
	table := fmt.Sprintf("`%s`", fc.TableID)

	var unnestClauses []string
	if len(fc.UnnestClauses) > 0 {
		for _, uc := range fc.UnnestClauses {
			u := uc.Build()
			unnestClauses = append(unnestClauses, u)
		}
	}

	return strings.Join(append([]string{table}, unnestClauses...), defaultExpressionSeparator)
}

//Equal is comparison
func (fc *FromClause) Equal(other *FromClause) bool {

	if len(fc.UnnestClauses) != len(other.UnnestClauses) {
		return false
	}

	for i := range fc.UnnestClauses {
		if *fc.UnnestClauses[i] != *other.UnnestClauses[i] {
			return false
		}
	}

	return fc.TableID == other.TableID
}
