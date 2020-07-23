package expr

//go:generate go run github.com/pointlander/peg expr.peg
type Qexpr struct {
}

func GetExpr(s string) (*Parser, error) {

	parser := Parser{Buffer: s}

	parser.Init()
	err := parser.Parse()

	if err != nil {
		return nil, err
	}
	parser.Execute()

	return &parser, nil
}
