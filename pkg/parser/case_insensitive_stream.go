package parser

import (
	"unicode"

	"github.com/antlr4-go/antlr/v4"
)

type CaseInsensitiveStream struct {
	antlr.CharStream
}

func (c *CaseInsensitiveStream) LA(i int) int {
	res := c.CharStream.LA(i)
	if res <= 0 {
		return res
	}
	return int(unicode.ToUpper(rune(res)))
}
