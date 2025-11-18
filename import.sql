USE moviedb;

SET GLOBAL local_infile = 1;

DROP TABLE IF EXISTS staging_imdb_movies;

CREATE TABLE staging_imdb_movies (
    budget TEXT,
    genres TEXT,
    homepage TEXT,
    id TEXT,
    keywords TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity TEXT,
    production_companies TEXT,
    production_countries TEXT,
    release_date TEXT,
    revenue TEXT,
    runtime TEXT,
    spoken_languages TEXT,
    `status` TEXT,
    tagline TEXT,
    title TEXT,
    vote_average TEXT,
    vote_count TEXT
);

LOAD DATA LOCAL INFILE '/Users/mcman/Downloads/tmdb_5000_movies.csv'
INTO TABLE staging_imdb_movies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) as total_rows FROM staging_imdb_movies;
SELECT * FROM staging_imdb_movies;