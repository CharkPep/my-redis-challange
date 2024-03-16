package utils

import (
	"io"
	"math/rand"
	"time"
)

func RandomAlphanumericString(w io.Writer, len int) {
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len; i++ {
		chars := []byte{uint8(source.Intn(26) + 65), uint8(source.Intn(26) + 97), uint8(source.Intn(10) + 48)}
		char := chars[source.Intn(3)]
		w.Write([]byte{char})
	}
}
