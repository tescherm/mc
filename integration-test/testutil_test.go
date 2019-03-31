// +build integration

package integration_test

import "math/rand"

const alphanumBytes = "abcdefghijklmnopqrstuvwxyz0123456789"

// randAlphaNumericString generates a lowercase alphanumeric string of the given length
func randAlphaNumericString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphanumBytes[rand.Intn(len(alphanumBytes))]
	}
	return string(b)
}
