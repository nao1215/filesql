// Package sqlguard classifies SQL statements so callers can enforce read-only
// access. It is intentionally small and dependency-free: the root filesql
// package owns the public database/sql wrappers, and this package holds only the
// statement-inspection logic they rely on, keeping that concern out of the
// already-large root package.
package sqlguard

import "strings"

// isWriteKeyword reports whether word (uppercased) is a statement verb that
// mutates data, schema, or database/connection state. Besides the DML/DDL verbs
// it includes the SQLite-specific mutators ANALYZE, REINDEX, VACUUM and
// ATTACH/DETACH, which change state without one of the usual verbs.
func isWriteKeyword(word string) bool {
	switch word {
	case "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
		"TRUNCATE", "REPLACE", "UPSERT",
		"ANALYZE", "REINDEX", "VACUUM", "ATTACH", "DETACH":
		return true
	default:
		return false
	}
}

// IsWrite reports whether the SQL statement performs a write.
//
// It is intentionally conservative: a statement is rejected if a write keyword
// appears anywhere at the top level, so writes cannot be smuggled past a
// read-only API through SQL comments, common table expressions (WITH ...
// DELETE) or a RETURNING clause executed via Query/QueryRow. Keywords inside
// string literals, quoted identifiers, comments or parenthesized subqueries are
// ignored to avoid rejecting legitimate SELECTs.
//
// An assigning PRAGMA (e.g. "PRAGMA foreign_keys = ON") is also treated as a
// write because it changes connection/database state, while a reading PRAGMA
// such as "PRAGMA table_info(users)" or "PRAGMA foreign_keys" is allowed.
func IsWrite(query string) bool {
	words, hasTopLevelAssign := scanTopLevel(query)
	for i, word := range words {
		if isWriteKeyword(word) {
			return true
		}
		if i == 0 && word == "PRAGMA" && hasTopLevelAssign {
			return true
		}
	}
	return false
}

// scanTopLevel scans an SQL statement and returns the uppercased keywords that
// appear at parenthesis depth zero (skipping comments, string literals and
// quoted identifiers) together with whether an "=" appears at the top level.
// CTE subqueries live inside parentheses, so the main statement verb is always
// reported while the inner verbs of the WITH clause are not.
func scanTopLevel(query string) (words []string, hasTopLevelAssign bool) {
	var word strings.Builder
	depth := 0

	flush := func() {
		if word.Len() > 0 {
			words = append(words, strings.ToUpper(word.String()))
			word.Reset()
		}
	}

	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case c == '-' && i+1 < len(runes) && runes[i+1] == '-':
			// Line comment: skip to end of line.
			flush()
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(runes) && runes[i+1] == '*':
			// Block comment: skip to closing */.
			flush()
			i += 2
			for i+1 < len(runes) && (runes[i] != '*' || runes[i+1] != '/') {
				i++
			}
			i++ // position on '/'; loop's i++ moves past it
		case c == '\'' || c == '"' || c == '`':
			// String literal or quoted identifier: skip to the matching quote,
			// honoring doubled-quote escapes ('' "" ``).
			flush()
			quote := c
			i++
			for i < len(runes) {
				if runes[i] == quote {
					if i+1 < len(runes) && runes[i+1] == quote {
						i++ // escaped quote, stay inside
					} else {
						break
					}
				}
				i++
			}
		case c == '(':
			flush()
			depth++
		case c == ')':
			flush()
			if depth > 0 {
				depth--
			}
		case c == '=':
			flush()
			if depth == 0 {
				hasTopLevelAssign = true
			}
		case isWordChar(c):
			if depth == 0 {
				word.WriteRune(c)
			}
		default:
			flush()
		}
	}
	flush()
	return words, hasTopLevelAssign
}

// isWordChar reports whether c can be part of an SQL identifier/keyword.
func isWordChar(c rune) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}
