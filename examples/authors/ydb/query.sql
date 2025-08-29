-- name: ListAuthors :many 
SELECT * FROM authors;

-- name: GetAuthor :one
SELECT * FROM authors
WHERE id = $p0;

-- name: GetAuthorsByName :many
SELECT * FROM authors
WHERE name = $p0;

-- name: ListAuthorsWithNullBio :many
SELECT * FROM authors
WHERE bio IS NULL;

-- name: Count :one
SELECT COUNT(*) FROM authors;

-- name: UpsertAuthor :queryrows
UPSERT INTO authors (id, name, bio) VALUES ($p0, $p1, $p2) RETURNING *;

-- name: DeleteAuthor :exec 
DELETE FROM authors WHERE id = $p0;

-- name: UpdateAuthorByID :one
UPDATE authors SET name = $p0, bio = $p1 WHERE id = $p2 RETURNING *;
