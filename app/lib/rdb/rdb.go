package rdb

import (
	"encoding/base64"
)

type Rdb struct{}

var (
	RDBRAW         = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	EMPTYRDBRAW, _ = base64.StdEncoding.DecodeString(RDBRAW)
)

func GetEmpty() []byte {
	return EMPTYRDBRAW
}
