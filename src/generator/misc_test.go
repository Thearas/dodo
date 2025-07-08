package generator

import "testing"

func TestRandomStr(t *testing.T) {
	for range 1000 {
		RandomStr(0, 10)
	}
}
