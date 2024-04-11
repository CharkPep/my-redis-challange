package encoding

import (
	"bufio"
	"fmt"
)

var ()

type ValueType byte

func (v ValueType) Parse(r *bufio.Reader) (N int, err error) {
	switch byte(v) {
	case STRING:
		return
	default:
		return 0, fmt.Errorf("parsing for a given value type: %q is not implemented", v)
	}
}
