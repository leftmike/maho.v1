package token

import (
	"testing"
)

func TestIsOpRune(t *testing.T) {
	for r := rune(0); r < 2000; r++ {
		switch r {
		case Minus,	Plus, Star, Slash, Percent, Equal, Less, Greater,
			Ampersand, Bar, Bang:
			if IsOpRune(r) != true {
				t.Errorf("IsOpRune('%c') got false want true", r)
			}
		default:
			if IsOpRune(r) != false {
				t.Errorf("IsOpRune('%c') got true want false", r)
			}
		}
	}

	for _, s := range operators {
		for _, r := range s {
			if IsOpRune(r) != true {
				t.Errorf("IsOpRune('%c') got false want true", r)
			}
		}
	}
}
