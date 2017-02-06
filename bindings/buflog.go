package bindings

import (
	"bufio"
	"bytes"
	"log"
	"testing"
)

func BufferedLogger(t *testing.T) (*log.Logger, func()) {
	b := bytes.NewBuffer(nil)
	l := log.New(b, "", log.Lshortfile|log.Ltime)
	buf := bufio.NewReader(b)
	f := func() {
		for {
			line, e := buf.ReadString('\n')
			if e != nil {
				break
			}
			t.Logf(`dump %s`, line)
		}
	}
	return l, f
}
