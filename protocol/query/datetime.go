package query

import (
	"fmt"
	"github.com/odpf/predator/util"
	"time"
)

//Expression is any expression
type Expression interface {
	Build() (string, error)
	implementExpression()
}

//TruncType is trunc type of date_trunc() and timestamp_trunc() function
type TruncType string

func (t TruncType) String() string {
	return string(t)
}

var TruncTypeHour TruncType = "HOUR"
var TruncTypeDay TruncType = "DAY"
var TruncTypeMonth TruncType = "MONTH"
var TruncTypeYear TruncType = "YEAR"

type TimestampValue struct {
	Value string
}

func (t *TimestampValue) Build() (string, error) {
	return util.DoubleQuote(t.Value), nil
}

func (t *TimestampValue) implementExpression() {
}

type FieldIdentifier struct {
	FieldID string
}

func (f *FieldIdentifier) Build() (string, error) {
	return f.FieldID, nil
}

func (f *FieldIdentifier) implementExpression() {
}

//Date is DATE function, it takes any timestamp expression
type Date struct {
	Year  int
	Month int
	Day   int

	TimestampExpr Expression
	Timezone      *time.Location
}

func (d *Date) Build() (string, error) {
	tz := d.Timezone
	if d.Timezone == nil {
		tz = time.UTC
	}

	if d.TimestampExpr != nil {
		tzString := util.DoubleQuote(tz.String())
		timestampExpr, err := d.TimestampExpr.Build()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("DATE(%s,%s)", timestampExpr, tzString), nil
	}

	return fmt.Sprintf("DATE(%d,%d,%d)", d.Year, d.Month, d.Day), nil
}

func (d *Date) implementExpression() {
}

//DateTrunc is type of DATE_TRUNC() function in bigquery SQL syntax
type DateTrunc struct {
	Target       Expression
	TruncateType TruncType
}

func (d *DateTrunc) Build() (string, error) {
	targetStr, err := d.Target.Build()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DATE_TRUNC(%s,%s)", targetStr, d.TruncateType.String()), nil
}

func (d *DateTrunc) implementExpression() {
}

type TimestampTrunc struct {
	TimestampExpr Expression
	TruncateType  TruncType
	Timezone      *time.Location
}

func (t *TimestampTrunc) Build() (string, error) {
	targetStr, err := t.TimestampExpr.Build()
	if err != nil {
		return "", err
	}

	tz := t.Timezone
	if t.Timezone == nil {
		tz = time.UTC
	}
	tzString := util.DoubleQuote(tz.String())

	return fmt.Sprintf("TIMESTAMP_TRUNC(%s,%s,%s)", targetStr, t.TruncateType.String(), tzString), nil

}

func (t *TimestampTrunc) implementExpression() {
}
