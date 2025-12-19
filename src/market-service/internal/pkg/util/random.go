package util

import (
	"crypto/rand"
	"math/big"
)

func SecureRandom(min, max int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(max-min+1))
	if err != nil {
		return min
	}
	return n.Int64() + min
}
