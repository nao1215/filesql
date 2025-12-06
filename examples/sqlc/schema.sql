-- Schema definition for sqlc code generation
-- Note: This schema must match the CSV file structure

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INTEGER NOT NULL
);
