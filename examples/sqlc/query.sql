-- name: GetUser :one
SELECT * FROM users WHERE id = ? LIMIT 1;

-- name: ListUsers :many
SELECT * FROM users ORDER BY name;

-- name: ListUsersByAge :many
SELECT * FROM users WHERE age >= ? ORDER BY age;

-- name: CountUsers :one
SELECT COUNT(*) as count FROM users;

-- name: GetAverageAge :one
SELECT AVG(age) as average_age FROM users;
