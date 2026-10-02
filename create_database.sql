create table movie (
	id integer primary key AUTOINCREMENT,
    title text,
    language text,
    image_url text,
    budget bigint,
    imdb_rating decimal(3,1),
    year int
);

create table director (
	id integer primary key AUTOINCREMENT,
    name text
);

create table director_movie (
	director_id int,
    movie_id int,
    foreign key (director_id) references director(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table genre (
	id integer primary key AUTOINCREMENT,
    name text
);

create table genre_movie (
	genre_id int,
    movie_id int,
    foreign key (genre_id) references genre(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table actor (
	id integer primary key AUTOINCREMENT,
    name text
);

create table actor_movie (
	actor_id int,
    movie_id int,
    character_name text,
    foreign key (actor_id) references actor(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table crew (
	id integer primary key AUTOINCREMENT,
    name text
);

create table crew_movie (
	crew_id int,
    movie_id int,
    foreign key (crew_id) references crew(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table production_company (
	id integer primary key AUTOINCREMENT,
    name text
);

create table production_company_movie (
	production_company_id int,
    movie_id int,
    foreign key (production_company_id) references production_company(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table user (
	id integer primary key AUTOINCREMENT,
    username text unique
);


create table user_password (
	user_id int,
    password_hash text,
    foreign key (user_id) references user(id)
);


create table session (
	id integer primary key AUTOINCREMENT,
    user_id int,
    created_at datetime not null default current_timestamp,
    foreign key (user_id) references user(id)
);


create table follow (
	follower_id int,
    followee_id int,
    foreign key (follower_id) references user(id),
    foreign key (followee_id) references user(id),
    primary key (follower_id, followee_id)
);

create table movie_rating (
	user_id int,
    movie_id int,
    rating decimal(3,1),
    review text,
    jumpscare_count integer CHECK (jumpscare_count IS NULL OR jumpscare_count >= 0),
    created_at datetime not null default current_timestamp,
    foreign key (user_id) references user(id),
    foreign key (movie_id) references movie(id)
    
);


create table favorite_director (
	user_id int,
    director_id int,
    foreign key (user_id) references user(id),
    foreign key (director_id) references director(id)
);

create table favorite_actor (
	user_id int,
    actor_id int,
    foreign key (user_id) references user(id),
    foreign key (actor_id) references actor(id)
);

create table movie_quote (
	id integer primary key AUTOINCREMENT,
    quote text not null
);

create table favorite_movie_quote (
	user_id int,
    quote_id int,
    foreign key (user_id) references user(id),
    foreign key (quote_id) references movie_quote(id)
);

-- Migration ledger for versioned database changes.
create table schema_migrations (
    version text primary key,
    applied_at datetime not null default current_timestamp,
    description text not null
);

-- A count may only be recorded against a movie currently classified as Horror.
create trigger movie_rating_jumpscare_horror_insert
before insert on movie_rating
when new.jumpscare_count is not null
 and not exists (
    select 1
    from genre_movie gm
    join genre g on g.id = gm.genre_id
    where gm.movie_id = new.movie_id
      and lower(trim(g.name)) = 'horror'
 )
begin
    select raise(abort, 'jumpscare_count is only valid for Horror movies');
end;

create trigger movie_rating_jumpscare_horror_update
before update of movie_id, jumpscare_count on movie_rating
when new.jumpscare_count is not null
 and not exists (
    select 1
    from genre_movie gm
    join genre g on g.id = gm.genre_id
    where gm.movie_id = new.movie_id
      and lower(trim(g.name)) = 'horror'
 )
begin
    select raise(abort, 'jumpscare_count is only valid for Horror movies');
end;

create trigger genre_movie_horror_delete_guard
before delete on genre_movie
when exists (
    select 1
    from genre g
    join movie_rating mr on mr.movie_id = old.movie_id
    where g.id = old.genre_id
      and lower(trim(g.name)) = 'horror'
      and mr.jumpscare_count is not null
 )
begin
    select raise(abort, 'cannot remove Horror genre while jumpscare reports exist');
end;

create trigger genre_movie_horror_update_guard
before update on genre_movie
when exists (
    select 1
    from genre g
    join movie_rating mr on mr.movie_id = old.movie_id
    where g.id = old.genre_id
      and lower(trim(g.name)) = 'horror'
      and mr.jumpscare_count is not null
 )
 and (new.movie_id <> old.movie_id or not exists (
    select 1 from genre g
    where g.id = new.genre_id and lower(trim(g.name)) = 'horror'
 ))
begin
    select raise(abort, 'cannot reassign Horror genre while jumpscare reports exist');
end;

create trigger genre_horror_rename_guard
before update of name on genre
when lower(trim(old.name)) = 'horror'
 and lower(trim(new.name)) <> 'horror'
 and exists (
    select 1
    from genre_movie gm
    join movie_rating mr on mr.movie_id = gm.movie_id
    where gm.genre_id = old.id
      and mr.jumpscare_count is not null
 )
begin
    select raise(abort, 'cannot rename Horror genre while jumpscare reports exist');
end;

create trigger genre_horror_delete_guard
before delete on genre
when lower(trim(old.name)) = 'horror'
 and exists (
    select 1
    from genre_movie gm
    join movie_rating mr on mr.movie_id = gm.movie_id
    where gm.genre_id = old.id
      and mr.jumpscare_count is not null
 )
begin
    select raise(abort, 'cannot delete Horror genre while jumpscare reports exist');
end;

insert into schema_migrations (version, description)
values ('001_add_jumpscare_tracking', 'Add Horror-only per-review jumpscare reporting');



