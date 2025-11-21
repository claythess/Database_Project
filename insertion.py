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
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    # verify
    cursor.execute("select * from movie limit 5;")
    rows = cursor.fetchall()
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
    # verify
    cursor.execute("select * from director limit 5;")
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
    # verify
    cursor.execute("select * from director_movie limit 5;")
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
    cursor.execute("select * from actor limit 5;")
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_actor_movie(conn):
    cursor = conn.cursor(dictionary=True)
    # temp table
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_actor_movie (
            actor_name TEXT,
            movie_title TEXT,
            character_name TEXT
        );
    """)
    # get data
    sql = """
    select c.title, c.cast
    from staging_imdb_credits c
    inner join movie m on c.title = m.title;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    # prepare data
    actor_movie_pairs = []
    for row in rows:
        actors_characters = json_processing.extract_actor_character(row)
        for actor_name, character_name in actors_characters:
            actor_str = str(actor_name).strip().title() if actor_name else ""
            title_str = str(row['title']).strip().title() if row['title'] else ""
            char_str = str(character_name).strip().title() if character_name else ""
            if actor_str and title_str and char_str:
                actor_movie_pairs.append((actor_str, title_str, char_str))

    print(f"Inserting {len(actor_movie_pairs)} actor-movie relationships...")
    # bulk insert to temp
    temp_insert = """
    INSERT INTO temp_actor_movie (actor_name, movie_title, character_name)
    VALUES (%s, %s, %s);
    """
    cursor.executemany(temp_insert, actor_movie_pairs)
    print("Temp table populated, now joining to actual tables...")
    # insert into final
    final_insert = """
    INSERT IGNORE INTO actor_movie(actor_id, movie_id, character_name)
    SELECT a.id, m.id, t.character_name
    FROM temp_actor_movie t
    INNER JOIN actor a ON TRIM(a.name) = TRIM(t.actor_name)
    INNER JOIN movie m ON TRIM(m.title) = TRIM(t.movie_title);
    """
    cursor.execute(final_insert)
    print(f"Inserted {cursor.rowcount} rows into actor_movie")
    conn.commit()
    # drop
    cursor.execute("DROP TEMPORARY TABLE temp_actor_movie;")
    # verify
    cursor.execute("SELECT * FROM actor_movie LIMIT 5;")
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
    cursor.execute("select * from production_company limit 5;")
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_production_company_movie(conn):
    cursor = conn.cursor(dictionary=True)
    # temp table
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_production_company_movie (
            company_name TEXT,
            movie_title TEXT
        );
    """)
    # get data
    sql = """
    select m.title, s.production_companies
    from staging_imdb_movies s
    inner join movie m on s.title = m.title;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    # data for temp table
    company_movie_pairs = []
    for row in rows:
        companies = json_processing.extract_company(row)
        for company in companies:
            company_str = str(company).strip() if company else ""
            title_str = str(row['title']).strip() if row['title'] else ""
            
            if company_str and title_str:
                company_movie_pairs.append((company_str, title_str))

    print(f"Inserting {len(company_movie_pairs)} company-movie relationships...")
    # bulk insert into temp table in batches
    temp_insert = """
    INSERT INTO temp_production_company_movie (company_name, movie_title)
    VALUES (%s, %s);
    """
    batch_size = 5000
    for i in range(0, len(company_movie_pairs), batch_size):
        batch = company_movie_pairs[i:i+batch_size]
        cursor.executemany(temp_insert, batch)
    print("Temp table populated, now joining to actual tables...")
    # insert to final
    final_insert = """
    INSERT IGNORE INTO production_company_movie(movie_id, production_company_id)
    SELECT m.id, pc.id
    FROM temp_production_company_movie t
    INNER JOIN production_company pc ON TRIM(pc.name) = TRIM(t.company_name)
    INNER JOIN movie m ON TRIM(m.title) = TRIM(t.movie_title);
    """
    cursor.execute(final_insert)
    print(f"Inserted {cursor.rowcount} rows into production_company_movie")
    conn.commit()
    # drop temp
    cursor.execute("DROP TEMPORARY TABLE temp_production_company_movie;")
    # verify
    cursor.execute("SELECT * FROM production_company_movie LIMIT 5;")
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
    cursor.execute("select * from genre limit 5;")
    rows = cursor.fetchall()
    cursor.close()
    return rows

def insert_genre_movie(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_genre_movie (
            genre_name TEXT,
            movie_title TEXT
        );
    """)
    sql = """
    select m.title, s.genres
    from staging_imdb_movies s
    inner join movie m on s.title = m.title;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    genre_movie_pairs = []
    for row in rows:
        genres = json_processing.extract_genre(row)
        for genre in genres:
            genre_str = str(genre).strip() if genre else ""
            title_str = str(row['title']).strip() if row['title'] else ""
            
            if genre_str and title_str:
                genre_movie_pairs.append((genre_str, title_str))

    print(f"Inserting {len(genre_movie_pairs)} genre-movie relationships...")
    temp_insert = """
    INSERT INTO temp_genre_movie (genre_name, movie_title)
    VALUES (%s, %s);
    """
    batch_size = 5000
    for i in range(0, len(genre_movie_pairs), batch_size):
        batch = genre_movie_pairs[i:i+batch_size]
        cursor.executemany(temp_insert, batch)    
    print("Temp table populated, now joining to actual tables...")
    final_insert = """
    INSERT IGNORE INTO genre_movie(movie_id, genre_id)
    SELECT m.id, g.id
    FROM temp_genre_movie t
    INNER JOIN genre g ON TRIM(g.name) = TRIM(t.genre_name)
    INNER JOIN movie m ON TRIM(m.title) = TRIM(t.movie_title);
    """
    cursor.execute(final_insert)
    print(f"Inserted {cursor.rowcount} rows into genre_movie")
    conn.commit()
    cursor.execute("DROP TEMPORARY TABLE temp_genre_movie;")
    cursor.execute("SELECT * FROM genre_movie LIMIT 5;")
    result = cursor.fetchall()
    cursor.close()
    return result

def insert_crew(conn):
    crew_sql = """
    select crew from staging_imdb_credits;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(crew_sql)
    json_rows = cursor.fetchall()
    crew_list = json_processing.process_json(json_rows, json_processing.extract_crew)
    sql = """
    insert into crew(name) values (%s);
    """
    data = [(crew_member,) for crew_member in crew_list]
    cursor.executemany(sql, data)
    conn.commit()
    # verify
    cursor.execute("select * from crew limit 5;")
    rows = cursor.fetchall()
    cursor.close()
    return rows


def insert_crew_movie(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_crew_movie (
            crew_name TEXT,
            movie_title TEXT
        );
    """)
    sql = """
    select c.title, c.crew
    from staging_imdb_credits c
    inner join movie m on c.title = m.title;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    crew_movie_pairs = []
    for row in rows:
        crew_members = json_processing.extract_crew(row)
        for crew_name in crew_members:
            crew_str = str(crew_name).strip() if crew_name else ""
            title_str = str(row['title']).strip() if row['title'] else ""
            
            if crew_str and title_str:
                crew_movie_pairs.append((crew_str, title_str))
    print(f"Inserting {len(crew_movie_pairs)} crew-movie relationships...")
    temp_insert = """
    INSERT INTO temp_crew_movie (crew_name, movie_title)
    VALUES (%s, %s);
    """
    batch_size = 5000
    for i in range(0, len(crew_movie_pairs), batch_size):
        batch = crew_movie_pairs[i:i+batch_size]
        cursor.executemany(temp_insert, batch)
    print("Temp table populated, now joining to actual tables...")
    final_insert = """
    INSERT IGNORE INTO crew_movie(movie_id, crew_id)
    SELECT m.id, c.id
    FROM temp_crew_movie t
    INNER JOIN crew c ON TRIM(c.name) = TRIM(t.crew_name)
    INNER JOIN movie m ON TRIM(m.title) = TRIM(t.movie_title);
    """
    cursor.execute(final_insert)
    print(f"Inserted {cursor.rowcount} rows into crew_movie")
    conn.commit()
    cursor.execute("DROP TEMPORARY TABLE temp_crew_movie;")
    cursor.execute("SELECT * FROM crew_movie LIMIT 5;")
    result = cursor.fetchall()
    cursor.close()
    return result

def main():
    conn = connect_db()

    for row in insert_movie(conn):
        pprint(row)

    for row in insert_director(conn):
        pprint(row)
    for row in insert_director_movie(conn):
        pprint(row)

    for row in insert_genre(conn):
        pprint(row)
    for row in insert_genre_movie(conn):
        pprint(row)

    for row in insert_actor(conn):
        pprint(row)
    for row in insert_actor_movie(conn):
        pprint(row)

    for row in insert_company(conn):
        pprint(row)
    for row in insert_production_company_movie(conn):
        pprint(row)

    for row in insert_crew(conn):
        pprint(row)
    for row in insert_crew_movie(conn):
        pprint(row)
        
    conn.close()

if __name__ == "__main__":
    main()
