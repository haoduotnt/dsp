package bindings

import (
	"fmt"
	"strconv"
	"testing"
)

func TestRetryWithSharding(t *testing.T) {
	cb := func(n int, args interface{}) (string, error) {
		t.Log(n, "called with", args)
		switch n {
		case 0:
			fallthrough
		case 1:
			fallthrough
		case 3:
			return "", fmt.Errorf("failing %d", n)
		case 2:
			return "", nil
		case 4:
			return "test", nil
		default:
			return "", fmt.Errorf("unhandled %d", n)
		}
	}

	r := &CountingCache{Callback: cb}
	an := &ShardSystem{Children: []CacheSystem{r}}
	sh := &ShardSystem{Children: []CacheSystem{r, r}, Fallback: an}

	rc1 := &RandomCache{an}
	rc2 := &RandomCache{sh}

	str := "test"
	id, err := rc1.FindID(str)
	t.Log("recieved id and err", id, err)
	if val, err := rc2.Load(strconv.Itoa(id)); err != nil {
		t.Error(err)
	} else if val != str {
		t.Error("incorrect return val")
	}
}

func TestNonIntSharding(t *testing.T) {
	cb := func(n int, args interface{}) (string, error) {
		t.Log(n, "called with", args)
		return "world", nil
	}
	r := &CountingCache{Callback: cb}
	sh := &ShardSystem{Children: []CacheSystem{r, r}}
	sh.Store("hello", "world")
	out, err := sh.Load("hello")
	t.Log(err)
	if out != "world" {
		t.Error("unmet expectation")
	}
}
