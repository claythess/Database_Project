# Sick Movie Application

This is a letterbox like application for rating movies. Users can also follow each other, and keep track of there favorite actor/director.

This app allows users to
- Search for movies
- Review Movies
- Set Favorite actor/director
- View favorite actor/director filmography
- Add friends
- View friends reviews/favorites


## [ER Diagram](ER-Diagram.png)

## [Video recording of your app](https://www.youtube.com/watch?v=BSUPeHYVc_w)

## Database upgrades

The Flask application uses SQLite (`movie.db`). The SQL import scripts under the project root are for data loading and are not a substitute for versioned application migrations.

For a new database, create it from `create_database.sql`; that schema includes the current migration ledger and Horror-only jumpscare constraints. For an existing database, apply each file in `migrations/` once and in version order. Before deploying this update:

1. Stop the application and make a backup of the target SQLite database.
2. Check that `SELECT version FROM schema_migrations WHERE version = '001_add_jumpscare_tracking';` returns no rows. If `schema_migrations` does not exist yet, this is the pre-migration database.
3. Apply `migrations/001_add_jumpscare_tracking.sql` to the target database using a SQLite client. For example, from the project directory: `sqlite3 movie.db < migrations/001_add_jumpscare_tracking.sql`.
4. Verify that the migration ledger contains `001_add_jumpscare_tracking`, `movie_rating` has a nullable `jumpscare_count` column, and the six migration triggers exist. Then restart the application.

The migration is transactional and records its version only after the schema change succeeds. Do not apply a version a second time. To roll back, restore the backup taken before applying it; this avoids relying on SQLite-version-specific column-removal behavior. Apply the same migration file to each server's own database and retain the ledger with that database.

Jumpscare totals are optional, nonnegative whole-number reports on a user's review and are accepted only when the movie is in the Horror genre. The database triggers enforce that rule as well as the application. Genre stats count each reviewed movie once for every genre assigned to it; ratings and director rankings use the user's latest review for each movie. Director average rankings show up to five directors with rated movies.

## User statistics

Open **View my stats** from your profile, or visit `/user/<username>/stats`. The page includes watched and rated movie counts, overall and per-genre average ratings, top five directors by watched movies, and top directors by average rating (only directors with at least three rated movies qualify). It also includes every movie tied for the user's highest rating, last reviewed date, Horror jumpscare reports, and the user's top ten Horror movies by reported jumpscares.

## Display theme

All pages share the responsive movie-stats visual theme. The color mode follows the operating system by default. Use the **Theme** control in the upper corner to cycle between system, light, and dark; an explicit selection is saved in the browser and can be reset to system mode with the same control.
