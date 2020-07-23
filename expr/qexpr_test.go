package expr_test

import (
	"testing"

	"github.com/kazu/vfs-index/expr"
	"github.com/stretchr/testify/assert"
)

func TestName(t *testing.T) {

	data := `name == "ほげ"`

	parser := expr.Parser{Buffer: data}

	parser.Init()
	err := parser.Parse()

	if err != nil {
		t.Error(err)
		//return
	}
	parser.Execute()

	assert.NoError(t, err)
	assert.Equal(t, "name", parser.Column)
	assert.Equal(t, "==", parser.Op)
	assert.Equal(t, "ほげ", parser.Value)
}

func TestSearch(t *testing.T) {

	data := `name.search("ほげ")`

	parser := expr.Parser{Buffer: data}

	parser.Init()
	err := parser.Parse()

	if err != nil {
		t.Error(err)
		//return
	}
	parser.Execute()

	assert.NoError(t, err)
	assert.Equal(t, "name", parser.Column)
	assert.Equal(t, "search", parser.Op)
	assert.Equal(t, "ほげ", parser.Value)
}
