package parser

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/token"
)

// Precedence levels, low binding to high. Every operator's binding power is
// decided here and nowhere else: the expression parser reads this table, and no
// other part of the package may reason about what binds tighter than what.
//
// The levels are the union of what the supported dialects need. Where two
// dialects disagree about one operator -- MySQL puts XOR between OR and AND,
// PostgreSQL has no XOR at all and puts "^" at exponentiation -- the
// disagreement is in binaryPrec, which reads the dialect, rather than in
// separate tables that would drift apart.
const (
	precLowest = iota
	precOr
	precXor
	precAnd
	precNot
	precIs
	precCompare
	precBetween
	// precOther is PostgreSQL's "any other operator": one level for the
	// bitwise, concatenation and JSON operators, below addition.
	precOther
	precBitOr
	precBitAnd
	precShift
	precAddSub
	precMulDiv
	precBitXor
	precConcat
	precCollate
	precUnary
	precCast
	precPostfix
)

// binaryOpFor reads the infix operator at the cursor, if there is one. It
// returns the operator, its precedence, whether it associates to the right, and
// how many tokens it spans. A word operator such as LIKE is matched only when
// it is a bare word, so a column named "like" written in quotes is a column.
func (p *Parser) binaryOpFor() (op ast.BinaryOp, prec int, tokens int, ok bool) {
	t := p.cur()
	if t.Kind == token.Op {
		if op, ok := symbolOperator(p.dialect, t.Text); ok {
			return op, binaryPrec(p.dialect, op), 1, true
		}
		return 0, 0, 0, false
	}
	if t.Kind != token.Word {
		return 0, 0, 0, false
	}
	switch upper(t.Text) {
	case kwAnd:
		return ast.And, precAnd, 1, true
	case "OR":
		return ast.Or, precOr, 1, true
	case "XOR":
		if p.dialect == dialects.MySQL {
			return ast.Xor, precXor, 1, true
		}
	case "DIV":
		if p.dialect == dialects.MySQL {
			return ast.IntDiv, precMulDiv, 1, true
		}
	case "MOD":
		// MOD is the operator unless a parenthesis follows the word with
		// nothing between them, which is how MySQL itself tells "MOD(a, b)"
		// from "a MOD (b + 1)".
		if p.dialect == dialects.MySQL && !(p.peek(1).IsOp("(") && p.adjacent(1)) {
			return ast.Mod, precMulDiv, 1, true
		}
	case "LIKE":
		return ast.Like, precCompare, 1, true
	case "ILIKE":
		if p.dialect == dialects.PostgreSQL {
			return ast.ILike, precCompare, 1, true
		}
	case "RLIKE":
		if p.dialect == dialects.MySQL {
			return ast.Regexp, precCompare, 1, true
		}
	case "REGEXP":
		if p.dialect == dialects.MySQL {
			return ast.Regexp, precCompare, 1, true
		}
	case "SIMILAR":
		if p.dialect == dialects.PostgreSQL && p.peek(1).IsWord("TO") {
			return ast.SimilarTo, precCompare, 2, true
		}
	case "SOUNDS":
		if p.dialect == dialects.MySQL && p.peek(1).IsWord("LIKE") {
			return ast.SoundsLike, precCompare, 2, true
		}
	case "MEMBER":
		if p.dialect == dialects.MySQL && p.peek(1).IsWord("OF") {
			return ast.MemberOf, precCompare, 2, true
		}
	}
	return 0, 0, 0, false
}

// symbolOperator maps an operator token to its meaning in a dialect. An
// operator a dialect does not have is not an operator there: MySQL's "||" is a
// logical OR and PostgreSQL's is concatenation, and neither dialect has the
// other's meaning.
func symbolOperator(d dialects.Dialect, text string) (ast.BinaryOp, bool) {
	switch text {
	case "+":
		return ast.Add, true
	case "-":
		return ast.Sub, true
	case "*":
		return ast.Mul, true
	case "/":
		return ast.Div, true
	case "%":
		return ast.Mod, true
	case "=", "==":
		return ast.Eq, true
	case "<>", "!=":
		return ast.NotEq, true
	case "<":
		return ast.Lt, true
	case "<=":
		return ast.Lte, true
	case ">":
		return ast.Gt, true
	case ">=":
		return ast.Gte, true
	case "&":
		return ast.BitAnd, true
	case "|":
		return ast.BitOr, true
	case "<<":
		return ast.ShiftLeft, true
	case ">>":
		return ast.ShiftRight, true
	case "||":
		if d == dialects.MySQL {
			return ast.Or, true
		}
		return ast.Concat, true
	case "&&":
		if d == dialects.MySQL {
			return ast.And, true
		}
	case "<=>":
		if d == dialects.MySQL {
			return ast.NullSafeEq, true
		}
	case "^":
		switch d {
		case dialects.MySQL, dialects.GoogleSQL:
			return ast.BitXor, true
		case dialects.PostgreSQL:
			return ast.Power, true
		case dialects.SQLite:
			return 0, false
		}
	case "#":
		if d == dialects.PostgreSQL {
			return ast.BitXor, true
		}
	case "~":
		if d == dialects.PostgreSQL {
			return ast.Regexp, true
		}
	case "~*":
		if d == dialects.PostgreSQL {
			return ast.RegexpCI, true
		}
	case "!~":
		if d == dialects.PostgreSQL {
			return ast.NotRegexp, true
		}
	case "!~*":
		if d == dialects.PostgreSQL {
			return ast.NotRegexpCI, true
		}
	case "~~":
		if d == dialects.PostgreSQL {
			return ast.Like, true
		}
	case "~~*":
		if d == dialects.PostgreSQL {
			return ast.ILike, true
		}
	case "!~~":
		if d == dialects.PostgreSQL {
			return ast.NotLike, true
		}
	case "!~~*":
		if d == dialects.PostgreSQL {
			return ast.NotILike, true
		}
	case "->":
		if d == dialects.PostgreSQL {
			return ast.JSONGet, true
		}
	case "->>":
		if d == dialects.PostgreSQL {
			return ast.JSONGetText, true
		}
	case "#>":
		if d == dialects.PostgreSQL {
			return ast.JSONPathGet, true
		}
	case "#>>":
		if d == dialects.PostgreSQL {
			return ast.JSONPathGetText, true
		}
	case "@>":
		if d == dialects.PostgreSQL {
			return ast.JSONContains, true
		}
	case "<@":
		if d == dialects.PostgreSQL {
			return ast.JSONContainedBy, true
		}
	case "@?":
		if d == dialects.PostgreSQL {
			return ast.JSONPathExists, true
		}
	case "@@":
		if d == dialects.PostgreSQL {
			return ast.JSONPathMatch, true
		}
	case "#-":
		if d == dialects.PostgreSQL {
			return ast.JSONPathDelete, true
		}
	}
	return 0, false
}

// binaryPrec is the binding power of an operator in a dialect.
func binaryPrec(d dialects.Dialect, op ast.BinaryOp) int {
	// PostgreSQL gives one level to everything its grammar calls "any other
	// operator", and puts that level below addition: "a || b * c" concatenates
	// the product, and "a # b + c" takes the XOR of the sum. The other two
	// dialects give each of these operators a level of its own.
	if d == dialects.PostgreSQL && postgresOtherOperator(op) {
		return precOther
	}
	switch op {
	case ast.Or:
		return precOr
	case ast.Xor:
		return precXor
	case ast.And:
		return precAnd
	case ast.Eq, ast.NotEq, ast.Lt, ast.Lte, ast.Gt, ast.Gte, ast.NullSafeEq:
		return precCompare
	case ast.Like, ast.NotLike, ast.ILike, ast.NotILike, ast.SimilarTo, ast.NotSimilarTo,
		ast.Regexp, ast.NotRegexp, ast.RegexpCI, ast.NotRegexpCI, ast.SoundsLike, ast.MemberOf:
		return precCompare
	case ast.BitOr:
		return precBitOr
	case ast.BitAnd:
		return precBitAnd
	case ast.ShiftLeft, ast.ShiftRight:
		return precShift
	case ast.Add, ast.Sub:
		return precAddSub
	case ast.Mul, ast.Div, ast.Mod, ast.IntDiv:
		return precMulDiv
	case ast.BitXor:
		// MySQL's "^" binds tighter than multiplication; PostgreSQL's "#" is an
		// ordinary operator at the level its manual calls "any other operator",
		// which sits with addition.
		if d == dialects.MySQL {
			return precBitXor
		}
		return precAddSub
	case ast.Power:
		// PostgreSQL raises to a power at a level of its own, above
		// multiplication.
		return precBitXor
	case ast.Concat:
		// BigQuery concatenates at the level it multiplies at; MySQL reads
		// "||" as OR and never reaches here.
		if d == dialects.GoogleSQL {
			return precMulDiv
		}
		return precConcat
	case ast.JSONGet, ast.JSONGetText, ast.JSONPathGet, ast.JSONPathGetText,
		ast.JSONContains, ast.JSONContainedBy, ast.JSONExists, ast.JSONPathExists,
		ast.JSONPathMatch, ast.JSONPathDelete:
		return precConcat
	default:
		return precCompare
	}
}

// postgresOtherOperator reports whether PostgreSQL's grammar reads an operator
// at its "any other operator" level, which sits below addition and above the
// pattern predicates.
func postgresOtherOperator(op ast.BinaryOp) bool {
	switch op {
	case ast.Concat, ast.BitXor, ast.BitAnd, ast.BitOr, ast.ShiftLeft, ast.ShiftRight,
		ast.JSONGet, ast.JSONGetText, ast.JSONPathGet, ast.JSONPathGetText,
		ast.JSONContains, ast.JSONContainedBy, ast.JSONExists, ast.JSONPathExists,
		ast.JSONPathMatch, ast.JSONPathDelete:
		return true
	default:
		return false
	}
}
