package expr

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

const endSymbol rune = 1114112

/* The rule types inferred from the grammar are below. */
type pegRule uint8

const (
	ruleUnknown pegRule = iota
	rulescheme
	ruleexpr
	rulesearch
	rulenum_op
	ruleop
	ruleeq_op
	rulele_op
	rulelt_op
	rulege_op
	rulegt_op
	ruleand
	rulecolumn
	rulevalue
	ruleraw
	ruleident
	rulespacing
	rulespace
	ruleend_of_file
	ruleAction0
	ruleAction1
	ruleAction2
	ruleAction3
	ruleAction4
	ruleAction5
	ruleAction6
	ruleAction7
	ruleAction8
	rulePegText

	rulePre
	ruleIn
	ruleSuf
)

var rul3s = [...]string{
	"Unknown",
	"scheme",
	"expr",
	"search",
	"num_op",
	"op",
	"eq_op",
	"le_op",
	"lt_op",
	"ge_op",
	"gt_op",
	"and",
	"column",
	"value",
	"raw",
	"ident",
	"spacing",
	"space",
	"end_of_file",
	"Action0",
	"Action1",
	"Action2",
	"Action3",
	"Action4",
	"Action5",
	"Action6",
	"Action7",
	"Action8",
	"PegText",

	"Pre_",
	"_In_",
	"_Suf",
}

type node32 struct {
	token32
	up, next *node32
}

func (node *node32) print(depth int, buffer string) {
	for node != nil {
		for c := 0; c < depth; c++ {
			fmt.Printf(" ")
		}
		fmt.Printf("\x1B[34m%v\x1B[m %v\n", rul3s[node.pegRule], strconv.Quote(string(([]rune(buffer)[node.begin:node.end]))))
		if node.up != nil {
			node.up.print(depth+1, buffer)
		}
		node = node.next
	}
}

func (node *node32) Print(buffer string) {
	node.print(0, buffer)
}

type element struct {
	node *node32
	down *element
}

/* ${@} bit structure for abstract syntax tree */
type token32 struct {
	pegRule
	begin, end, next uint32
}

func (t *token32) isZero() bool {
	return t.pegRule == ruleUnknown && t.begin == 0 && t.end == 0 && t.next == 0
}

func (t *token32) isParentOf(u token32) bool {
	return t.begin <= u.begin && t.end >= u.end && t.next > u.next
}

func (t *token32) getToken32() token32 {
	return token32{pegRule: t.pegRule, begin: uint32(t.begin), end: uint32(t.end), next: uint32(t.next)}
}

func (t *token32) String() string {
	return fmt.Sprintf("\x1B[34m%v\x1B[m %v %v %v", rul3s[t.pegRule], t.begin, t.end, t.next)
}

type tokens32 struct {
	tree    []token32
	ordered [][]token32
}

func (t *tokens32) trim(length int) {
	t.tree = t.tree[0:length]
}

func (t *tokens32) Print() {
	for _, token := range t.tree {
		fmt.Println(token.String())
	}
}

func (t *tokens32) Order() [][]token32 {
	if t.ordered != nil {
		return t.ordered
	}

	depths := make([]int32, 1, math.MaxInt16)
	for i, token := range t.tree {
		if token.pegRule == ruleUnknown {
			t.tree = t.tree[:i]
			break
		}
		depth := int(token.next)
		if length := len(depths); depth >= length {
			depths = depths[:depth+1]
		}
		depths[depth]++
	}
	depths = append(depths, 0)

	ordered, pool := make([][]token32, len(depths)), make([]token32, len(t.tree)+len(depths))
	for i, depth := range depths {
		depth++
		ordered[i], pool, depths[i] = pool[:depth], pool[depth:], 0
	}

	for i, token := range t.tree {
		depth := token.next
		token.next = uint32(i)
		ordered[depth][depths[depth]] = token
		depths[depth]++
	}
	t.ordered = ordered
	return ordered
}

type state32 struct {
	token32
	depths []int32
	leaf   bool
}

func (t *tokens32) AST() *node32 {
	tokens := t.Tokens()
	stack := &element{node: &node32{token32: <-tokens}}
	for token := range tokens {
		if token.begin == token.end {
			continue
		}
		node := &node32{token32: token}
		for stack != nil && stack.node.begin >= token.begin && stack.node.end <= token.end {
			stack.node.next = node.up
			node.up = stack.node
			stack = stack.down
		}
		stack = &element{node: node, down: stack}
	}
	return stack.node
}

func (t *tokens32) PreOrder() (<-chan state32, [][]token32) {
	s, ordered := make(chan state32, 6), t.Order()
	go func() {
		var states [8]state32
		for i := range states {
			states[i].depths = make([]int32, len(ordered))
		}
		depths, state, depth := make([]int32, len(ordered)), 0, 1
		write := func(t token32, leaf bool) {
			S := states[state]
			state, S.pegRule, S.begin, S.end, S.next, S.leaf = (state+1)%8, t.pegRule, t.begin, t.end, uint32(depth), leaf
			copy(S.depths, depths)
			s <- S
		}

		states[state].token32 = ordered[0][0]
		depths[0]++
		state++
		a, b := ordered[depth-1][depths[depth-1]-1], ordered[depth][depths[depth]]
	depthFirstSearch:
		for {
			for {
				if i := depths[depth]; i > 0 {
					if c, j := ordered[depth][i-1], depths[depth-1]; a.isParentOf(c) &&
						(j < 2 || !ordered[depth-1][j-2].isParentOf(c)) {
						if c.end != b.begin {
							write(token32{pegRule: ruleIn, begin: c.end, end: b.begin}, true)
						}
						break
					}
				}

				if a.begin < b.begin {
					write(token32{pegRule: rulePre, begin: a.begin, end: b.begin}, true)
				}
				break
			}

			next := depth + 1
			if c := ordered[next][depths[next]]; c.pegRule != ruleUnknown && b.isParentOf(c) {
				write(b, false)
				depths[depth]++
				depth, a, b = next, b, c
				continue
			}

			write(b, true)
			depths[depth]++
			c, parent := ordered[depth][depths[depth]], true
			for {
				if c.pegRule != ruleUnknown && a.isParentOf(c) {
					b = c
					continue depthFirstSearch
				} else if parent && b.end != a.end {
					write(token32{pegRule: ruleSuf, begin: b.end, end: a.end}, true)
				}

				depth--
				if depth > 0 {
					a, b, c = ordered[depth-1][depths[depth-1]-1], a, ordered[depth][depths[depth]]
					parent = a.isParentOf(b)
					continue
				}

				break depthFirstSearch
			}
		}

		close(s)
	}()
	return s, ordered
}

func (t *tokens32) PrintSyntax() {
	tokens, ordered := t.PreOrder()
	max := -1
	for token := range tokens {
		if !token.leaf {
			fmt.Printf("%v", token.begin)
			for i, leaf, depths := 0, int(token.next), token.depths; i < leaf; i++ {
				fmt.Printf(" \x1B[36m%v\x1B[m", rul3s[ordered[i][depths[i]-1].pegRule])
			}
			fmt.Printf(" \x1B[36m%v\x1B[m\n", rul3s[token.pegRule])
		} else if token.begin == token.end {
			fmt.Printf("%v", token.begin)
			for i, leaf, depths := 0, int(token.next), token.depths; i < leaf; i++ {
				fmt.Printf(" \x1B[31m%v\x1B[m", rul3s[ordered[i][depths[i]-1].pegRule])
			}
			fmt.Printf(" \x1B[31m%v\x1B[m\n", rul3s[token.pegRule])
		} else {
			for c, end := token.begin, token.end; c < end; c++ {
				if i := int(c); max+1 < i {
					for j := max; j < i; j++ {
						fmt.Printf("skip %v %v\n", j, token.String())
					}
					max = i
				} else if i := int(c); i <= max {
					for j := i; j <= max; j++ {
						fmt.Printf("dupe %v %v\n", j, token.String())
					}
				} else {
					max = int(c)
				}
				fmt.Printf("%v", c)
				for i, leaf, depths := 0, int(token.next), token.depths; i < leaf; i++ {
					fmt.Printf(" \x1B[34m%v\x1B[m", rul3s[ordered[i][depths[i]-1].pegRule])
				}
				fmt.Printf(" \x1B[34m%v\x1B[m\n", rul3s[token.pegRule])
			}
			fmt.Printf("\n")
		}
	}
}

func (t *tokens32) PrintSyntaxTree(buffer string) {
	tokens, _ := t.PreOrder()
	for token := range tokens {
		for c := 0; c < int(token.next); c++ {
			fmt.Printf(" ")
		}
		fmt.Printf("\x1B[34m%v\x1B[m %v\n", rul3s[token.pegRule], strconv.Quote(string(([]rune(buffer)[token.begin:token.end]))))
	}
}

func (t *tokens32) Add(rule pegRule, begin, end, depth uint32, index int) {
	t.tree[index] = token32{pegRule: rule, begin: uint32(begin), end: uint32(end), next: uint32(depth)}
}

func (t *tokens32) Tokens() <-chan token32 {
	s := make(chan token32, 16)
	go func() {
		for _, v := range t.tree {
			s <- v.getToken32()
		}
		close(s)
	}()
	return s
}

func (t *tokens32) Error() []token32 {
	ordered := t.Order()
	length := len(ordered)
	tokens, length := make([]token32, length), length-1
	for i := range tokens {
		o := ordered[length-i]
		if len(o) > 1 {
			tokens[i] = o[len(o)-2].getToken32()
		}
	}
	return tokens
}

func (t *tokens32) Expand(index int) {
	tree := t.tree
	if index >= len(tree) {
		expanded := make([]token32, 2*len(tree))
		copy(expanded, tree)
		t.tree = expanded
	}
}

type Parser struct {
	Qexpr

	Buffer string
	buffer []rune
	rules  [29]func() bool
	Parse  func(rule ...int) error
	Reset  func()
	Pretty bool
	tokens32
}

type textPosition struct {
	line, symbol int
}

type textPositionMap map[int]textPosition

func translatePositions(buffer []rune, positions []int) textPositionMap {
	length, translations, j, line, symbol := len(positions), make(textPositionMap, len(positions)), 0, 1, 0
	sort.Ints(positions)

search:
	for i, c := range buffer {
		if c == '\n' {
			line, symbol = line+1, 0
		} else {
			symbol++
		}
		if i == positions[j] {
			translations[positions[j]] = textPosition{line, symbol}
			for j++; j < length; j++ {
				if i != positions[j] {
					continue search
				}
			}
			break search
		}
	}

	return translations
}

type parseError struct {
	p   *Parser
	max token32
}

func (e *parseError) Error() string {
	tokens, error := []token32{e.max}, "\n"
	positions, p := make([]int, 2*len(tokens)), 0
	for _, token := range tokens {
		positions[p], p = int(token.begin), p+1
		positions[p], p = int(token.end), p+1
	}
	translations := translatePositions(e.p.buffer, positions)
	format := "parse error near %v (line %v symbol %v - line %v symbol %v):\n%v\n"
	if e.p.Pretty {
		format = "parse error near \x1B[34m%v\x1B[m (line %v symbol %v - line %v symbol %v):\n%v\n"
	}
	for _, token := range tokens {
		begin, end := int(token.begin), int(token.end)
		error += fmt.Sprintf(format,
			rul3s[token.pegRule],
			translations[begin].line, translations[begin].symbol,
			translations[end].line, translations[end].symbol,
			strconv.Quote(string(e.p.buffer[begin:end])))
	}

	return error
}

func (p *Parser) PrintSyntaxTree() {
	p.tokens32.PrintSyntaxTree(p.Buffer)
}

func (p *Parser) Highlighter() {
	p.PrintSyntax()
}

func (p *Parser) Execute() {
	buffer, _buffer, text, begin, end := p.Buffer, p.buffer, "", 0, 0
	for token := range p.Tokens() {
		switch token.pegRule {

		case rulePegText:
			begin, end = int(token.begin), int(token.end)
			text = string(_buffer[begin:end])

		case ruleAction0:
			p.PushOp("search")
		case ruleAction1:
			p.Push()
		case ruleAction2:
			p.Op = "=="
		case ruleAction3:
			p.Op = "<="
		case ruleAction4:
			p.Op = "<"
		case ruleAction5:
			p.Op = ">="
		case ruleAction6:
			p.Op = ">"
		case ruleAction7:
			p.Column = text
		case ruleAction8:
			p.Value = text

		}
	}
	_, _, _, _, _ = buffer, _buffer, text, begin, end
}

func (p *Parser) Init() {
	p.buffer = []rune(p.Buffer)
	if len(p.buffer) == 0 || p.buffer[len(p.buffer)-1] != endSymbol {
		p.buffer = append(p.buffer, endSymbol)
	}

	tree := tokens32{tree: make([]token32, math.MaxInt16)}
	var max token32
	position, depth, tokenIndex, buffer, _rules := uint32(0), uint32(0), 0, p.buffer, p.rules

	p.Parse = func(rule ...int) error {
		r := 1
		if len(rule) > 0 {
			r = rule[0]
		}
		matches := p.rules[r]()
		p.tokens32 = tree
		if matches {
			p.trim(tokenIndex)
			return nil
		}
		return &parseError{p, max}
	}

	p.Reset = func() {
		position, tokenIndex, depth = 0, 0, 0
	}

	add := func(rule pegRule, begin uint32) {
		tree.Expand(tokenIndex)
		tree.Add(rule, begin, position, depth, tokenIndex)
		tokenIndex++
		if begin != position && position > max.end {
			max = token32{rule, begin, position, depth}
		}
	}

	matchDot := func() bool {
		if buffer[position] != endSymbol {
			position++
			return true
		}
		return false
	}

	/*matchChar := func(c byte) bool {
		if buffer[position] == c {
			position++
			return true
		}
		return false
	}*/

	_rules = [...]func() bool{
		nil,
		/* 0 scheme <- <(expr (spacing and spacing expr)* end_of_file)> */
		func() bool {
			position0, tokenIndex0, depth0 := position, tokenIndex, depth
			{
				position1 := position
				depth++
				if !_rules[ruleexpr]() {
					goto l0
				}
			l2:
				{
					position3, tokenIndex3, depth3 := position, tokenIndex, depth
					if !_rules[rulespacing]() {
						goto l3
					}
					if !_rules[ruleand]() {
						goto l3
					}
					if !_rules[rulespacing]() {
						goto l3
					}
					if !_rules[ruleexpr]() {
						goto l3
					}
					goto l2
				l3:
					position, tokenIndex, depth = position3, tokenIndex3, depth3
				}
				if !_rules[ruleend_of_file]() {
					goto l0
				}
				depth--
				add(rulescheme, position1)
			}
			return true
		l0:
			position, tokenIndex, depth = position0, tokenIndex0, depth0
			return false
		},
		/* 1 expr <- <(num_op / search)> */
		func() bool {
			position4, tokenIndex4, depth4 := position, tokenIndex, depth
			{
				position5 := position
				depth++
				{
					position6, tokenIndex6, depth6 := position, tokenIndex, depth
					if !_rules[rulenum_op]() {
						goto l7
					}
					goto l6
				l7:
					position, tokenIndex, depth = position6, tokenIndex6, depth6
					if !_rules[rulesearch]() {
						goto l4
					}
				}
			l6:
				depth--
				add(ruleexpr, position5)
			}
			return true
		l4:
			position, tokenIndex, depth = position4, tokenIndex4, depth4
			return false
		},
		/* 2 search <- <(column '.' ('s' 'e' 'a' 'r' 'c' 'h' '(') value ')' Action0)> */
		func() bool {
			position8, tokenIndex8, depth8 := position, tokenIndex, depth
			{
				position9 := position
				depth++
				if !_rules[rulecolumn]() {
					goto l8
				}
				if buffer[position] != rune('.') {
					goto l8
				}
				position++
				if buffer[position] != rune('s') {
					goto l8
				}
				position++
				if buffer[position] != rune('e') {
					goto l8
				}
				position++
				if buffer[position] != rune('a') {
					goto l8
				}
				position++
				if buffer[position] != rune('r') {
					goto l8
				}
				position++
				if buffer[position] != rune('c') {
					goto l8
				}
				position++
				if buffer[position] != rune('h') {
					goto l8
				}
				position++
				if buffer[position] != rune('(') {
					goto l8
				}
				position++
				if !_rules[rulevalue]() {
					goto l8
				}
				if buffer[position] != rune(')') {
					goto l8
				}
				position++
				if !_rules[ruleAction0]() {
					goto l8
				}
				depth--
				add(rulesearch, position9)
			}
			return true
		l8:
			position, tokenIndex, depth = position8, tokenIndex8, depth8
			return false
		},
		/* 3 num_op <- <(column spacing op spacing value Action1)> */
		func() bool {
			position10, tokenIndex10, depth10 := position, tokenIndex, depth
			{
				position11 := position
				depth++
				if !_rules[rulecolumn]() {
					goto l10
				}
				if !_rules[rulespacing]() {
					goto l10
				}
				if !_rules[ruleop]() {
					goto l10
				}
				if !_rules[rulespacing]() {
					goto l10
				}
				if !_rules[rulevalue]() {
					goto l10
				}
				if !_rules[ruleAction1]() {
					goto l10
				}
				depth--
				add(rulenum_op, position11)
			}
			return true
		l10:
			position, tokenIndex, depth = position10, tokenIndex10, depth10
			return false
		},
		/* 4 op <- <(eq_op / le_op / lt_op / ge_op / gt_op)> */
		func() bool {
			position12, tokenIndex12, depth12 := position, tokenIndex, depth
			{
				position13 := position
				depth++
				{
					position14, tokenIndex14, depth14 := position, tokenIndex, depth
					if !_rules[ruleeq_op]() {
						goto l15
					}
					goto l14
				l15:
					position, tokenIndex, depth = position14, tokenIndex14, depth14
					if !_rules[rulele_op]() {
						goto l16
					}
					goto l14
				l16:
					position, tokenIndex, depth = position14, tokenIndex14, depth14
					if !_rules[rulelt_op]() {
						goto l17
					}
					goto l14
				l17:
					position, tokenIndex, depth = position14, tokenIndex14, depth14
					if !_rules[rulege_op]() {
						goto l18
					}
					goto l14
				l18:
					position, tokenIndex, depth = position14, tokenIndex14, depth14
					if !_rules[rulegt_op]() {
						goto l12
					}
				}
			l14:
				depth--
				add(ruleop, position13)
			}
			return true
		l12:
			position, tokenIndex, depth = position12, tokenIndex12, depth12
			return false
		},
		/* 5 eq_op <- <('=' '=' Action2)> */
		func() bool {
			position19, tokenIndex19, depth19 := position, tokenIndex, depth
			{
				position20 := position
				depth++
				if buffer[position] != rune('=') {
					goto l19
				}
				position++
				if buffer[position] != rune('=') {
					goto l19
				}
				position++
				if !_rules[ruleAction2]() {
					goto l19
				}
				depth--
				add(ruleeq_op, position20)
			}
			return true
		l19:
			position, tokenIndex, depth = position19, tokenIndex19, depth19
			return false
		},
		/* 6 le_op <- <('<' '=' Action3)> */
		func() bool {
			position21, tokenIndex21, depth21 := position, tokenIndex, depth
			{
				position22 := position
				depth++
				if buffer[position] != rune('<') {
					goto l21
				}
				position++
				if buffer[position] != rune('=') {
					goto l21
				}
				position++
				if !_rules[ruleAction3]() {
					goto l21
				}
				depth--
				add(rulele_op, position22)
			}
			return true
		l21:
			position, tokenIndex, depth = position21, tokenIndex21, depth21
			return false
		},
		/* 7 lt_op <- <('<' Action4)> */
		func() bool {
			position23, tokenIndex23, depth23 := position, tokenIndex, depth
			{
				position24 := position
				depth++
				if buffer[position] != rune('<') {
					goto l23
				}
				position++
				if !_rules[ruleAction4]() {
					goto l23
				}
				depth--
				add(rulelt_op, position24)
			}
			return true
		l23:
			position, tokenIndex, depth = position23, tokenIndex23, depth23
			return false
		},
		/* 8 ge_op <- <('>' '=' Action5)> */
		func() bool {
			position25, tokenIndex25, depth25 := position, tokenIndex, depth
			{
				position26 := position
				depth++
				if buffer[position] != rune('>') {
					goto l25
				}
				position++
				if buffer[position] != rune('=') {
					goto l25
				}
				position++
				if !_rules[ruleAction5]() {
					goto l25
				}
				depth--
				add(rulege_op, position26)
			}
			return true
		l25:
			position, tokenIndex, depth = position25, tokenIndex25, depth25
			return false
		},
		/* 9 gt_op <- <('>' Action6)> */
		func() bool {
			position27, tokenIndex27, depth27 := position, tokenIndex, depth
			{
				position28 := position
				depth++
				if buffer[position] != rune('>') {
					goto l27
				}
				position++
				if !_rules[ruleAction6]() {
					goto l27
				}
				depth--
				add(rulegt_op, position28)
			}
			return true
		l27:
			position, tokenIndex, depth = position27, tokenIndex27, depth27
			return false
		},
		/* 10 and <- <('&' '&')> */
		func() bool {
			position29, tokenIndex29, depth29 := position, tokenIndex, depth
			{
				position30 := position
				depth++
				if buffer[position] != rune('&') {
					goto l29
				}
				position++
				if buffer[position] != rune('&') {
					goto l29
				}
				position++
				depth--
				add(ruleand, position30)
			}
			return true
		l29:
			position, tokenIndex, depth = position29, tokenIndex29, depth29
			return false
		},
		/* 11 column <- <(ident Action7)> */
		func() bool {
			position31, tokenIndex31, depth31 := position, tokenIndex, depth
			{
				position32 := position
				depth++
				if !_rules[ruleident]() {
					goto l31
				}
				if !_rules[ruleAction7]() {
					goto l31
				}
				depth--
				add(rulecolumn, position32)
			}
			return true
		l31:
			position, tokenIndex, depth = position31, tokenIndex31, depth31
			return false
		},
		/* 12 value <- <(('"' raw '"') / raw)> */
		func() bool {
			position33, tokenIndex33, depth33 := position, tokenIndex, depth
			{
				position34 := position
				depth++
				{
					position35, tokenIndex35, depth35 := position, tokenIndex, depth
					if buffer[position] != rune('"') {
						goto l36
					}
					position++
					if !_rules[ruleraw]() {
						goto l36
					}
					if buffer[position] != rune('"') {
						goto l36
					}
					position++
					goto l35
				l36:
					position, tokenIndex, depth = position35, tokenIndex35, depth35
					if !_rules[ruleraw]() {
						goto l33
					}
				}
			l35:
				depth--
				add(rulevalue, position34)
			}
			return true
		l33:
			position, tokenIndex, depth = position33, tokenIndex33, depth33
			return false
		},
		/* 13 raw <- <(ident Action8)> */
		func() bool {
			position37, tokenIndex37, depth37 := position, tokenIndex, depth
			{
				position38 := position
				depth++
				if !_rules[ruleident]() {
					goto l37
				}
				if !_rules[ruleAction8]() {
					goto l37
				}
				depth--
				add(ruleraw, position38)
			}
			return true
		l37:
			position, tokenIndex, depth = position37, tokenIndex37, depth37
			return false
		},
		/* 14 ident <- <<(!('=' / '>' / '<' / '(' / ')' / '"' / ' ' / '.' / '&') .)*>> */
		func() bool {
			{
				position40 := position
				depth++
				{
					position41 := position
					depth++
				l42:
					{
						position43, tokenIndex43, depth43 := position, tokenIndex, depth
						{
							position44, tokenIndex44, depth44 := position, tokenIndex, depth
							{
								position45, tokenIndex45, depth45 := position, tokenIndex, depth
								if buffer[position] != rune('=') {
									goto l46
								}
								position++
								goto l45
							l46:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('>') {
									goto l47
								}
								position++
								goto l45
							l47:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('<') {
									goto l48
								}
								position++
								goto l45
							l48:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('(') {
									goto l49
								}
								position++
								goto l45
							l49:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune(')') {
									goto l50
								}
								position++
								goto l45
							l50:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('"') {
									goto l51
								}
								position++
								goto l45
							l51:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune(' ') {
									goto l52
								}
								position++
								goto l45
							l52:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('.') {
									goto l53
								}
								position++
								goto l45
							l53:
								position, tokenIndex, depth = position45, tokenIndex45, depth45
								if buffer[position] != rune('&') {
									goto l44
								}
								position++
							}
						l45:
							goto l43
						l44:
							position, tokenIndex, depth = position44, tokenIndex44, depth44
						}
						if !matchDot() {
							goto l43
						}
						goto l42
					l43:
						position, tokenIndex, depth = position43, tokenIndex43, depth43
					}
					depth--
					add(rulePegText, position41)
				}
				depth--
				add(ruleident, position40)
			}
			return true
		},
		/* 15 spacing <- <space*> */
		func() bool {
			{
				position55 := position
				depth++
			l56:
				{
					position57, tokenIndex57, depth57 := position, tokenIndex, depth
					if !_rules[rulespace]() {
						goto l57
					}
					goto l56
				l57:
					position, tokenIndex, depth = position57, tokenIndex57, depth57
				}
				depth--
				add(rulespacing, position55)
			}
			return true
		},
		/* 16 space <- <(' ' / '\t')> */
		func() bool {
			position58, tokenIndex58, depth58 := position, tokenIndex, depth
			{
				position59 := position
				depth++
				{
					position60, tokenIndex60, depth60 := position, tokenIndex, depth
					if buffer[position] != rune(' ') {
						goto l61
					}
					position++
					goto l60
				l61:
					position, tokenIndex, depth = position60, tokenIndex60, depth60
					if buffer[position] != rune('\t') {
						goto l58
					}
					position++
				}
			l60:
				depth--
				add(rulespace, position59)
			}
			return true
		l58:
			position, tokenIndex, depth = position58, tokenIndex58, depth58
			return false
		},
		/* 17 end_of_file <- <!.> */
		func() bool {
			position62, tokenIndex62, depth62 := position, tokenIndex, depth
			{
				position63 := position
				depth++
				{
					position64, tokenIndex64, depth64 := position, tokenIndex, depth
					if !matchDot() {
						goto l64
					}
					goto l62
				l64:
					position, tokenIndex, depth = position64, tokenIndex64, depth64
				}
				depth--
				add(ruleend_of_file, position63)
			}
			return true
		l62:
			position, tokenIndex, depth = position62, tokenIndex62, depth62
			return false
		},
		/* 19 Action0 <- <{p.PushOp("search")}> */
		func() bool {
			{
				add(ruleAction0, position)
			}
			return true
		},
		/* 20 Action1 <- <{p.Push() }> */
		func() bool {
			{
				add(ruleAction1, position)
			}
			return true
		},
		/* 21 Action2 <- <{p.Op = "==" }> */
		func() bool {
			{
				add(ruleAction2, position)
			}
			return true
		},
		/* 22 Action3 <- <{p.Op = "<="}> */
		func() bool {
			{
				add(ruleAction3, position)
			}
			return true
		},
		/* 23 Action4 <- <{p.Op = "<"}> */
		func() bool {
			{
				add(ruleAction4, position)
			}
			return true
		},
		/* 24 Action5 <- <{p.Op = ">=" }> */
		func() bool {
			{
				add(ruleAction5, position)
			}
			return true
		},
		/* 25 Action6 <- <{p.Op = ">"}> */
		func() bool {
			{
				add(ruleAction6, position)
			}
			return true
		},
		/* 26 Action7 <- <{p.Column = text }> */
		func() bool {
			{
				add(ruleAction7, position)
			}
			return true
		},
		/* 27 Action8 <- <{p.Value = text}> */
		func() bool {
			{
				add(ruleAction8, position)
			}
			return true
		},
		nil,
	}
	p.rules = _rules
}
