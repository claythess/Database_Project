import mysql.connector
from pprint import pprint
import json_processing

# change this
def connect_db():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="moviedb"
    )

def insert_movie(conn):
    sql = """
    insert into movie (title, language, budget, imdb_rating, year)
    select distinct
        title, 
        original_language, 
        nullif(budget, ''),
        nullif(vote_average, ''),
        year(str_to_date(nullif(release_date, ''), '%Y-%m-%d'))
    from staging_imdb_movies;
    """
    sel = """
    select * from movie limit 5;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.execute(sel)
    cursor.close()
    return rows

def insert_director(conn):
    crew = """
    select crew from staging_imdb_credits;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(crew)
    json_rows = cursor.fetchall()
    director_list = json_processing.process_json(json_rows, json_processing.extract_director)
    sql = """
    insert into director(name) values (%s);
    """
    data = [(director,) for director in director_list]
    cursor.executemany(sql, data)
    conn.commit()
    sel = """
    select * from director limit 5;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_director_movie(conn):
    sql = """
    select c.title, c.crew
    from staging_imdb_credits c
    inner join movie m on c.title = m.title;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    # list of (movie_title, director_name) tuples
    director_movie_pairs = []
    for row in rows:
        directors = json_processing.extract_director(row)
        for director in directors:
            director_movie_pairs.append((row['title'], director))

    insert_sql = """
    insert into director_movie(movie_id, director_id)
    select m.id, d.id
    from movie m
    inner join director d
    where m.title = %s and d.name = %s;
    """
    cursor.executemany(insert_sql, director_movie_pairs)
    conn.commit()
    
    # Verify
    sel = """
    select * from director_movie limit 5;
    """
    cursor.execute(sel)
    result = cursor.fetchall()
    cursor.close()
    return result

def insert_actor(conn):
    cast = """
    select cast from staging_imdb_credits;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(cast)
    json_rows = cursor.fetchall()
    actor_list = json_processing.process_json(json_rows, json_processing.extract_actor)
    sql = """
    insert into actor(name) values (%s);
    """
    data = [(actor,) for actor in actor_list]
    cursor.executemany(sql, data)
    conn.commit()
    sel = """
    select * from actor limit 5;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_actor_movie(conn):
    sql = """
    select c.title, c.cast
    from staging_imdb_credits c
    inner join movie m on c.title = m.title;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    # list of (actor_name, movie_title, character_name) tuples
    actor_movie_pairs = []
    for row in rows:
        actors_characters = json_processing.extract_actor_character(row)
        for actor_name, character_name in actors_characters:
            actor_movie_pairs.append((actor_name.strip(), row['title'].strip(), character_name.strip()))

    insert_sql = """
    insert into actor_movie(actor_id, movie_id, character_name)
    select a.id, m.id, %s
    from movie m
    inner join actor a on trim(a.name) = %s
    where trim(m.title) = %s;
    """
    cursor.executemany(insert_sql, actor_movie_pairs)

    cursor.execute("SELECT ROW_COUNT()")
    print(f"Rows affected: {cursor.rowcount}")

    conn.commit()
    sel = """
    select * from actor_movie limit 5;
    """
    cursor.execute(sel)
    result = cursor.fetchall()
    cursor.close()
    return result

def insert_company(conn):
    companies = """
    select production_companies from staging_imdb_movies;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(companies)
    json_rows = cursor.fetchall()
    company_list = json_processing.process_json(json_rows, json_processing.extract_company)
    sql = """
    insert into production_company(name) values (%s);
    """
    data = [(company,) for company in company_list]
    cursor.executemany(sql, data)
    conn.commit()
    sel = """
    select * from production_company limit 5;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_production_company_movie(conn):
    sql = """
    select m.title, s.production_companies
    from staging_imdb_movies s
    inner join movie m on s.title = m.title;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    # list of (movie_title, company_name) tuples
    company_movie_pairs = []
    for row in rows:
        companies = json_processing.extract_company(row)
        for company in companies:
            company_movie_pairs.append((row['title'], company))

    insert_sql = """
    insert into production_company_movie(movie_id, production_company_id)
    select m.id, pc.id
    from movie m
    inner join production_company pc on pc.name = %s
    where m.title = %s;
    """
    print(f"Inserting {len(company_movie_pairs)} company-movie relationships...")
    cursor.executemany(insert_sql, company_movie_pairs)
    conn.commit()
    
    sel = """
    select * from production_company_movie limit 5;
    """
    cursor.execute(sel)
    result = cursor.fetchall()
    cursor.close()
    return result

def insert_genre(conn):
    genres = """
    select genres from staging_imdb_movies;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(genres)
    json_rows = cursor.fetchall()
    genre_list = json_processing.process_json(json_rows, json_processing.extract_genre)
    sql = """
    insert into genre(name) values (%s);
    """
    data = [(genre,) for genre in genre_list]
    cursor.executemany(sql, data)
    conn.commit()
    sel = """
    select * from genre limit 5;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_genre_movie(conn):
    sql = """
    select m.title, s.genres
    from staging_imdb_movies s
    inner join movie m on s.title = m.title;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    rows = cursor.fetchall()

    # list of (movie_title, genre_name) tuples
    genre_movie_pairs = []
    for row in rows:
        genres = json_processing.extract_genre(row)
        for genre in genres:
            genre_movie_pairs.append((row['title'], genre))

    insert_sql = """
    insert into genre_movie(movie_id, genre_id)
    select m.id, g.id
    from movie m
    inner join genre g on g.name = %s
    where m.title = %s;
    """
    print(f"Inserting {len(genre_movie_pairs)} genre-movie relationships...")
    cursor.executemany(insert_sql, genre_movie_pairs)
    conn.commit()
    
    sel = """
    select * from genre_movie limit 5;
    """
    cursor.execute(sel)
    result = cursor.fetchall()
    cursor.close()
    return result

def main():
    conn = connect_db()
    # for row in insert_movie(conn):
    #     pprint(row)
    # for row in insert_director(conn):
    #     pprint(row)
    # for row in insert_director_movie(conn):
    #     pprint(row)
    # for row in insert_genre(conn):
    #     pprint(row)
    # for row in insert_genre_movie(conn):
    #     pprint(row)
    for row in insert_actor(conn):
        pprint(row)
    # for row in insert_actor_movie(conn):
    #     pprint(row)
    # for row in insert_company(conn):
    #     pprint(row)
    # for row in insert_production_company_movie(conn):
    #     pprint(row)
    conn.close()

if __name__ == "__main__":
    main()
