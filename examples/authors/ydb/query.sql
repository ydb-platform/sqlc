-- name: CountAuthors :one
-- @ydb-retry-idempotent
SELECT COUNT(*) FROM authors;

-- name: Coalesce :many
SELECT id, name, COALESCE(bio, 'Null value!') FROM authors;

-- name: CreateOrUpdateAuthor :execresult 
-- @ydb-with-tx 
UPSERT INTO authors (id, name, bio) VALUES ($p0, $p1, $p2);

-- name: CreateOrUpdateAuthorReturningBio :one
UPSERT INTO authors (id, name, bio) VALUES ($p0, $p1, $p2) RETURNING bio;

-- name: DeleteAuthor :exec 
DELETE FROM authors WHERE id = $p0;

-- name: UpdateAuthorByID :one
UPDATE authors SET name = $p0, bio = $p1 WHERE id = $p2 RETURNING *;
