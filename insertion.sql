-- movie
insert into movie (title, language, budget, imdb_rating, year)
select distinct
	title, 
    original_language, 
    nullif(budget, ''),
    nullif(vote_average, ''),
    year(str_to_date(nullif(release_date, ''), '%Y-%m-%d'))
from staging_imdb_movies;

-- director
insert into director(name)
select distinct json_unquote(json_extract(crew_item, '$.name'))
from staging_imdb_credits,
     json_table(
        crew,
        '$[*]'
        columns(
            job varchar(100) path '$.job',
            crew_item json path '$'
        )
     ) as parsed
where json_valid(crew) = 1 and job = 'Director';

-- actor