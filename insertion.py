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
    select * from movie limit 1;
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
    select * from director limit 1;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

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
    select * from actor limit 1;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

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
    select * from production_company limit 1;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

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
    select * from genre limit 1;
    """
    cursor.execute(sel)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def main():
    conn = connect_db()
    # for row in insert_movie(conn):
    #     pprint(row)
    # for row in insert_director(conn):
    #     pprint(row)
    # for row in insert_genre(conn):
    #     pprint(row)
    # for row in insert_actor(conn):
    #     pprint(row)
    # for row in insert_company(conn):
    #     pprint(row)
    conn.close()

if __name__ == "__main__":
    main()
