package dsp_flights

import (
	"testing"
)

func TestB64(t *testing.T) {
	pt := []byte("Hello this is a test")

	p := &B64{Key: []byte("hello"), IV: []byte("whatwhat")}
	ct := p.Encrypt(pt)
	t.Logf("Ciphertext: %s\n", ct)

	sDec := "1Q_bm0NJ6agmxKY0gKvnPkjtQvc4u_lc"

	recovered_pt := p.Decrypt(sDec)
	t.Logf("Recovered plaintext: %s\n", recovered_pt)
}

func TestCT(t *testing.T) {
	t.Log((&B64{Key: []byte("hello"), IV: []byte("whatwhat")}).GetCT("hello"))
}
