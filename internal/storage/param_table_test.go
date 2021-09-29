package storage

import (
	"fmt"
	"testing"
)

func TestParamTable_compressType(t *testing.T) {
	Params.Init()
	compressType := Params.compressType
	fmt.Println(compressType)
}
