create database moviedb;

use moviedb;

select count(*) from movie;

drop table staging_imdb_movies;

create table movie (
	id int primary key,
    title text,
    language varchar(10),
    image_url text,
    budget bigint,
    imdb_rating decimal(2,1),
    year int
);

alter table movie modify column language text;

alter table movie modify column id int auto_increment;

create table director (
	id int primary key,
    name text
);
alter table director modify column id int auto_increment;

create table director_movie (
	director_id int,
    movie_id int,
    foreign key (director_id) references director(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table genre (
	id int primary key,
    name text
);
alter table genre modify column id int auto_increment;

create table genre_movie (
	genre_id int,
    movie_id int,
    foreign key (genre_id) references genre(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table actor (
	id int primary key,
    name text
);
alter table actor modify column id int auto_increment;

create table actor_movie (
	actor_id int,
    movie_id int,
    character_name text,
    foreign key (actor_id) references actor(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table crew (
	id int primary key,
    name text
);
alter table crew modify column id int auto_increment;

create table crew_movie (
	crew_id int,
    movie_id int,
    foreign key (crew_id) references crew(id),
    FOREIGN KEY (movie_id) references movie(id)
);

create table production_company (
	id int primary key,
    name text
);
alter table production_company modify column id int auto_increment;

create table production_company_movie (
	production_company_id int,
    movie_id int,
    foreign key (production_company_id) references production_company(id),
    FOREIGN KEY (movie_id) references movie(id)
);


-- User Stuff

create table user (
	id int primary key,
    username text
);

alter table user modify column id int auto_increment;

alter table user modify column username text unique;

create table user_password (
	user_id int,
    password_hash text,
    foreign key (user_id) references user(id)
);


create table session (
	id int primary key,
    user_id int,
    created_at datetime not null default current_timestamp,
    foreign key (user_id) references user(id)
);

alter table session modify column id int auto_increment;

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
    foreign key (user_id) references user(id),
    foreign key (movie_id) references movie(id)
);

alter table movie_rating add column created_at datetime not null default current_timestamp;

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



