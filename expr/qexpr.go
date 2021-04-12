package expr

//go:generate go run github.com/pointlander/peg expr.peg
type Expr struct {
	Column string
	Op     string
	Value  string
}

type Qexpr struct {
	Expr
	Ands []Expr
}

func (q *Qexpr) Push() {
	q.Ands = append(q.Ands, q.Expr)
}

func (q *Qexpr) PushOp(op string) {
	q.Expr.Op = op
	q.Ands = append(q.Ands, q.Expr)
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
