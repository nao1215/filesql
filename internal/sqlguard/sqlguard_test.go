package sqlguard

import "testing"

func TestIsWrite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		query string
		want  bool
	}{
		// Plain write statements.
		{"INSERT INTO users VALUES (1)", true},
		{"  INSERT INTO users VALUES (1)", true}, // leading space
		{"insert into users values (1)", true},   // lowercase
		{"INSERT\nINTO users VALUES (1)", true},  // newline after keyword
		{"UPDATE users SET name = 'a'", true},
		{"DELETE FROM users", true},
		{"DROP TABLE users", true},
		{"ALTER TABLE users ADD col TEXT", true},
		{"CREATE TABLE users (id INT)", true},
		{"TRUNCATE TABLE users", true},
		{"REPLACE INTO users VALUES (1)", true},
		{"UPSERT INTO users VALUES (1)", true},

		// Writes hidden behind comments.
		{"/*x*/ DELETE FROM users", true},
		{"-- comment\nDELETE FROM users", true},
		{"/* multi\nline */ UPDATE users SET a=1", true},
		{"  /*a*/ /*b*/ INSERT INTO users VALUES (1)", true},

		// Writes behind a CTE.
		{"WITH cte AS (SELECT 1) DELETE FROM users", true},
		{"WITH cte AS (SELECT 1) UPDATE users SET a = 1", true},
		{"WITH cte AS (SELECT 1) INSERT INTO users SELECT * FROM cte", true},
		{"WITH RECURSIVE cte(x) AS (SELECT 1) DELETE FROM users", true},

		// DML with RETURNING (returns rows but still mutates data).
		{"DELETE FROM users WHERE id = 1 RETURNING id", true},
		{"UPDATE users SET a = 1 RETURNING a", true},

		// SQLite statements that mutate state without a DML verb.
		{"VACUUM", true},
		{"vacuum", true},
		{"ANALYZE", true},
		{"ANALYZE users", true},
		{"REINDEX", true},
		{"ATTACH DATABASE 'other.db' AS other", true},
		{"DETACH DATABASE other", true},

		// PRAGMA that assigns a value mutates connection/database state.
		{"PRAGMA foreign_keys = ON", true},
		{"PRAGMA foreign_keys=ON", true},
		{"PRAGMA journal_mode = WAL", true},

		// Reading PRAGMAs stay allowed.
		{"PRAGMA foreign_keys", false},
		{"PRAGMA user_version", false},

		// Read statements.
		{"SELECT * FROM users", false},
		{"select * from users", false},
		{"  SELECT * FROM users", false},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", false},
		{"WITH cte AS (SELECT 1), d AS (SELECT 2) SELECT * FROM cte, d", false},
		{"EXPLAIN SELECT * FROM users", false},
		{"PRAGMA table_info(users)", false},
		{"-- delete is mentioned in a comment\nSELECT * FROM users", false},
		{"SELECT note FROM users WHERE note = 'please delete this'", false}, // keyword inside a string literal
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			if got := IsWrite(tt.query); got != tt.want {
				t.Errorf("IsWrite(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
