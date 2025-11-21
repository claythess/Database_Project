-- delete duplicates from staging tables
-- staging_imdb_credits
ALTER TABLE staging_imdb_credits ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;
DELETE FROM staging_imdb_credits
WHERE temp_id IN (
    SELECT temp_id FROM (
        SELECT temp_id, ROW_NUMBER() OVER (PARTITION BY title ORDER BY temp_id) as rn
        FROM staging_imdb_credits
    ) sub 
    WHERE rn > 1
);
ALTER TABLE staging_imdb_credits DROP COLUMN temp_id;
-- staging_imdb_movies
ALTER TABLE staging_imdb_movies ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;
DELETE FROM staging_imdb_movies
WHERE temp_id IN (
    SELECT temp_id FROM (
        SELECT temp_id, ROW_NUMBER() OVER (PARTITION BY title ORDER BY temp_id) as rn
        FROM staging_imdb_movies
    ) sub 
    WHERE rn > 1
);
ALTER TABLE staging_imdb_movies DROP COLUMN temp_id;

-- movie
alter table movie add constraint no_dup_movie unique (title(500));

-- director
alter table director add constraint no_dup_dir unique (name(500));

-- director movie
alter table director_movie add constraint pk_dir_movie primary key (director_id, movie_id);

-- genre
alter table genre add constraint no_dup_genre unique (name(500));

-- genre movie
alter table genre_movie add constraint pk_genre_movie primary key (genre_id, movie_id);

-- actor
alter table actor add constraint no_dup_actor unique (name(500));

-- actor movie
ALTER TABLE actor_movie ADD CONSTRAINT unique_actor_movie UNIQUE (actor_id, movie_id, character_name(500));

-- crew
alter table crew add constraint no_dup_crew unique (name(500));

-- crew movie
alter table crew_movie add constraint pk_crew_movie primary key (crew_id, movie_id);

-- production company
alter table production_company add constraint no_dup_prod_company unique (name(500));

-- production company movie
alter table production_company_movie add constraint pk_prod_company_movie primary key (production_company_id, movie_id);
