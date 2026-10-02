-- Migration: 001_add_jumpscare_tracking
-- Purpose: Add optional per-review jumpscare reports, restricted to Horror movies.
-- Database: SQLite 3.x
-- Apply once to an existing database created from create_database.sql.
-- Change management: back up the database first; apply this entire file in one
-- transaction; verify schema_migrations before/after. Do not re-run if recorded.
-- Rollback: restore the pre-migration backup. SQLite does not provide a portable
-- transactional DROP COLUMN workflow across all supported SQLite versions.

BEGIN IMMEDIATE;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT NOT NULL
);

-- Precondition: this version must not already appear in schema_migrations.
ALTER TABLE movie_rating
    ADD COLUMN jumpscare_count INTEGER
    CHECK (jumpscare_count IS NULL OR jumpscare_count >= 0);

CREATE TRIGGER movie_rating_jumpscare_horror_insert
BEFORE INSERT ON movie_rating
WHEN NEW.jumpscare_count IS NOT NULL
 AND NOT EXISTS (
    SELECT 1
    FROM genre_movie gm
    JOIN genre g ON g.id = gm.genre_id
    WHERE gm.movie_id = NEW.movie_id
      AND lower(trim(g.name)) = 'horror'
 )
BEGIN
    SELECT RAISE(ABORT, 'jumpscare_count is only valid for Horror movies');
END;

CREATE TRIGGER movie_rating_jumpscare_horror_update
BEFORE UPDATE OF movie_id, jumpscare_count ON movie_rating
WHEN NEW.jumpscare_count IS NOT NULL
 AND NOT EXISTS (
    SELECT 1
    FROM genre_movie gm
    JOIN genre g ON g.id = gm.genre_id
    WHERE gm.movie_id = NEW.movie_id
      AND lower(trim(g.name)) = 'horror'
 )
BEGIN
    SELECT RAISE(ABORT, 'jumpscare_count is only valid for Horror movies');
END;

CREATE TRIGGER genre_movie_horror_delete_guard
BEFORE DELETE ON genre_movie
WHEN EXISTS (
    SELECT 1
    FROM genre g
    JOIN movie_rating mr ON mr.movie_id = OLD.movie_id
    WHERE g.id = OLD.genre_id
      AND lower(trim(g.name)) = 'horror'
      AND mr.jumpscare_count IS NOT NULL
 )
BEGIN
    SELECT RAISE(ABORT, 'cannot remove Horror genre while jumpscare reports exist');
END;

CREATE TRIGGER genre_movie_horror_update_guard
BEFORE UPDATE ON genre_movie
WHEN EXISTS (
    SELECT 1
    FROM genre g
    JOIN movie_rating mr ON mr.movie_id = OLD.movie_id
    WHERE g.id = OLD.genre_id
      AND lower(trim(g.name)) = 'horror'
      AND mr.jumpscare_count IS NOT NULL
 )
 AND (NEW.movie_id <> OLD.movie_id OR NOT EXISTS (
    SELECT 1 FROM genre g
    WHERE g.id = NEW.genre_id AND lower(trim(g.name)) = 'horror'
 ))
BEGIN
    SELECT RAISE(ABORT, 'cannot reassign Horror genre while jumpscare reports exist');
END;

CREATE TRIGGER genre_horror_rename_guard
BEFORE UPDATE OF name ON genre
WHEN lower(trim(OLD.name)) = 'horror'
 AND lower(trim(NEW.name)) <> 'horror'
 AND EXISTS (
    SELECT 1
    FROM genre_movie gm
    JOIN movie_rating mr ON mr.movie_id = gm.movie_id
    WHERE gm.genre_id = OLD.id
      AND mr.jumpscare_count IS NOT NULL
 )
BEGIN
    SELECT RAISE(ABORT, 'cannot rename Horror genre while jumpscare reports exist');
END;

CREATE TRIGGER genre_horror_delete_guard
BEFORE DELETE ON genre
WHEN lower(trim(OLD.name)) = 'horror'
 AND EXISTS (
    SELECT 1
    FROM genre_movie gm
    JOIN movie_rating mr ON mr.movie_id = gm.movie_id
    WHERE gm.genre_id = OLD.id
      AND mr.jumpscare_count IS NOT NULL
 )
BEGIN
    SELECT RAISE(ABORT, 'cannot delete Horror genre while jumpscare reports exist');
END;

INSERT INTO schema_migrations (version, description)
VALUES ('001_add_jumpscare_tracking', 'Add Horror-only per-review jumpscare reporting');

COMMIT;