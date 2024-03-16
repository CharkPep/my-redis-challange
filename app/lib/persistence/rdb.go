package persistence

import (
	"encoding/base64"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

var (
	RDBRAW         = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	EMPTYRDBRAW, _ = base64.StdEncoding.DecodeString(RDBRAW)
)

func GetEmpty() *resp.Rdb {
	return &resp.Rdb{
		Content: EMPTYRDBRAW,
	}
}
