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



