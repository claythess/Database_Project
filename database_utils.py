import mysql.connector
from pprint import pprint
import hashlib
import requests

import logging
logging.basicConfig(level=logging.DEBUG)

with open("API-KEY",'r') as f:
    API_KEY=f.read()

def connect_db() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host="192.168.1.224",
        user="root",
        password="password",
        database="moviedb"
    )
    
conn = connect_db()
conn.autocommit = True

def make_hash(key):
    m = hashlib.sha256()
    m.update(key.encode("utf-8"))
    return m.hexdigest()

def get_movie_api(title: str, year):
   if year:
       url = f"http://www.omdbapi.com/?apikey={API_KEY}&t={title.replace(' ','+')}&y={year}&type=movie"
   else:
       url = f"http://www.omdbapi.com/?apikey={API_KEY}&t={title.replace(' ','+')}&type=movie"

   response = requests.get(url)
   return response.status_code, response.json()

def get_director(name) -> int:
    sql = """
    select id from director where lower(name) = lower(%s) limit 1;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (name,))
    ret = cursor.fetchone()
    if ret:
        return ret["id"]
    else:
        return None
    
def get_actor(name) -> int:
    sql = """
    select id from actor where lower(name) = lower(%s) limit 1;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (name,))
    ret = cursor.fetchone()
    if ret:
        return ret["id"]
    else:
        return None
    
def get_genre(name) -> int:
    sql = """
    select id from genre where lower(name) = lower(%s) limit 1;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (name,))
    ret = cursor.fetchone()
    if ret:
        return ret["id"]
    else:
        return None

def get_movie(title, year = None) -> dict:
    if year:
        sql = """
        select * from movie where lower(title) = lower(%s) and year = %s limit 1;
        """
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, (title, year))
    else:
        sql = """
        select * from movie where lower(title) like lower(%s) limit 1;
        """
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, (title,))
        
    ret = cursor.fetchone()
    logging.debug(ret)
    if ret:
        if ret["image_url"] == None:
            logging.debug("No Image URL, Fetching")
            status, movie_data = get_movie_api(title, year)
            if status != 200:
                logging.debug("Failed To Fetch")
                return ret

            sql = "update movie set image_url = %s where id = %s"
            cursor.execute(sql, (movie_data["Poster"], ret["id"]))

            sql = "select * from movie where id = %s"
            cursor.execute(sql, (ret["id"],))
            ret = cursor.fetchone()
            
        return ret
    else:
        # Movie doesnt exist, check API
        status, movie_data = get_movie_api(title, year)
        logging.debug(status)
        logging.debug(movie_data)
        
        if status != 200:
            return None
        # Insert stuff into movie
        title = movie_data["Title"]
        year = int(movie_data["Year"])
        language = movie_data["Language"]
        image_url = movie_data["Poster"]
        imdbRating = movie_data["imdbRating"]
        
        conn.autocommit = False
        
        sql = """insert into movie (title, language, image_url, imdb_rating, year) values (%s, %s, %s, nullif(%s,"N/A"), %s)"""
        cursor.execute(sql, (title, language, image_url, imdbRating, year))
        movie_id = cursor.lastrowid
        
        for director in movie_data["Director"].split(", "):
        
            director_id = get_director(director)
            
            if director_id == None:
                sql = """insert into director (name) values (%s)"""
                cursor.execute(sql, (director,))
                director_id = cursor.lastrowid
                
            sql = """insert into director_movie (movie_id, director_id) values (%s, %s)"""
            cursor.execute(sql, (movie_id, director_id))
        
        for actor in movie_data["Actors"].split(", "):
            actor_id = get_actor(actor)
            if actor_id == None:
                sql = """insert into actor (name) values (%s)"""
                cursor.execute(sql, (actor,))
                actor_id = cursor.lastrowid
                
            sql = """insert into actor_movie (movie_id, actor_id) values (%s, %s)"""
            cursor.execute(sql, (movie_id, actor_id))
            
        for genre in movie_data["Genre"].split(", "):
            genre_id = get_genre(genre)
            if genre_id == None:
                sql = """insert into genre (name) values (%s)"""
                cursor.execute(sql, (genre,))
                genre_id = cursor.lastrowid
                
            sql = """insert into genre_movie (movie_id, genre_id) values (%s, %s)"""
            cursor.execute(sql, (movie_id, genre_id))
        
        conn.commit()
        conn.autocommit=True
        
        return get_movie(title,year)
        
def get_movie_by_id(id) -> dict:
    sql = """
        select * from movie where id = %s;
        """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (id,))
    
    ret = cursor.fetchone()
    logging.debug(ret)
    if ret:
        if ret["image_url"] == None:
            logging.debug("No Image URL, Fetching")
            status, movie_data = get_movie_api(ret["title"], ret["year"])
            if status != 200 or not movie_data or "Poster" not in movie_data:
                logging.debug("Failed To Fetch")
                return ret

            sql = "update movie set image_url = %s where id = %s"
            cursor.execute(sql, (movie_data["Poster"], ret["id"]))

            sql = "select * from movie where id = %s"
            cursor.execute(sql, (ret["id"],))
            ret = cursor.fetchone()
            
        return ret
    else:
        return None
    
def get_movie_directors(movie_id):
    sql = """select d.name from movie m 
        join director_movie md on md.movie_id = m.id
        join director d on md.director_id = d.id
        where m.id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["name"] for i in ret]
    else:
        return []
    
def get_movie_actors(movie_id):
    sql = """select a.name from movie m 
        join actor_movie ma on ma.movie_id = m.id
        join actor a on ma.actor_id = a.id
        where m.id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["name"] for i in ret]
    else:
        return []
    
def get_movie_genres(movie_id):
    sql = """select g.name from movie m 
        join genre_movie mg on mg.movie_id = m.id
        join genre g on mg.genre_id = g.id
        where m.id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["name"] for i in ret]
    else:
        return []
    
def get_movie_production_company(movie_id):
    sql = """select p.name from movie m 
        join production_company_movie mp on mp.movie_id = m.id
        join production_company p on mp.production_company_id = p.id
        where m.id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["name"] for i in ret]
    else:
        return []

def get_movie_actors_full(movie_id):
    sql = """select a.id, a.name from actor a
        join actor_movie am on am.actor_id = a.id
        where am.movie_id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []

def get_movie_directors_full(movie_id):
    sql = """select d.id, d.name from director d
        join director_movie dm on dm.director_id = d.id
        where dm.movie_id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (movie_id,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []

def get_actor_by_id(actor_id):
    sql = "select id, name from actor where id = %s;"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (actor_id,))
    return cursor.fetchone()

def get_director_by_id(director_id):
    sql = "select id, name from director where id = %s;"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (director_id,))
    return cursor.fetchone()

def get_movies_by_actor(actor_id):
    sql = """select m.id, m.title, m.year, m.image_url from movie m
        join actor_movie am on am.movie_id = m.id
        where am.actor_id = %s
        order by m.year desc;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (actor_id,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []

def get_movies_by_director(director_id):
    sql = """select m.id, m.title, m.year, m.image_url from movie m
        join director_movie dm on dm.movie_id = m.id
        where dm.director_id = %s
        order by m.year desc;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (director_id,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []
        
def insert_review(user_id, movie_id, rating, review):
    sql = """insert into movie_rating (user_id, movie_id, rating, review) values (%s, %s, %s, %s);"""
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id, movie_id, rating, review))
    
def get_user_reviews(user_id):
    sql = """select * from movie_rating where user_id = %s order by created_at desc;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []


def get_favorite_actor(user_id):
    sql = """select a.id, a.name from favorite_actor fa join actor a on fa.actor_id = a.id where fa.user_id = %s limit 1;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchone()
    if ret:
        return ret
    else:
        return None


def get_favorite_director(user_id):
    sql = """select d.id, d.name from favorite_director fd join director d on fd.director_id = d.id where fd.user_id = %s limit 1;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchone()
    if ret:
        return ret
    else:
        return None


def set_favorite_actor_by_name(user_id, actor_name):
    # ensure actor exists
    actor_id = get_actor(actor_name)
    cursor = conn.cursor(dictionary=True)
    if actor_id is None:
        sql = "insert into actor (name) values (%s)"
        cursor.execute(sql, (actor_name,))
        actor_id = cursor.lastrowid

    # remove existing favorite and insert
    sql = "delete from favorite_actor where user_id = %s"
    cursor.execute(sql, (user_id,))
    sql = "insert into favorite_actor (user_id, actor_id) values (%s, %s)"
    cursor.execute(sql, (user_id, actor_id))


def set_favorite_director_by_name(user_id, director_name):
    director_id = get_director(director_name)
    cursor = conn.cursor(dictionary=True)
    if director_id is None:
        sql = "insert into director (name) values (%s)"
        cursor.execute(sql, (director_name,))
        director_id = cursor.lastrowid

    sql = "delete from favorite_director where user_id = %s"
    cursor.execute(sql, (user_id,))
    sql = "insert into favorite_director (user_id, director_id) values (%s, %s)"
    cursor.execute(sql, (user_id, director_id))
     

def get_user_id(username):
    """
    Should only be used after verify user
    """
    sql = "select id from user where username = %s;"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (username,))
    ret = cursor.fetchone()
    if ret:
        return ret['id']
    else:
        return None
    
def get_user_by_id(user_id) -> str:
    """
    Should only be used after verify user
    """
    sql = "select username from user where id = %s;"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchone()
    if ret:
        return ret['username']
    else:
        return None

def insert_follow(follower_id, folowee_id):
    sql = "insert into follow (follower_id, followee_id) values (%s, %s);"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (follower_id, folowee_id))

def get_followers(user_id):
    sql = """select u.username from follow f
            join user u on f.follower_id = u.id
            where f.followee_id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["username"] for i in ret]
    else:
        return []

def get_followees(user_id):
    sql = """select u.username from follow f
            join user u on f.followee_id = u.id
            where f.follower_id = %s;"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    ret = cursor.fetchall()
    if ret:
        return [i["username"] for i in ret]
    else:
        return []


def search_users(query):
    sql = """select id, username from user where lower(username) like lower(%s) limit 50;"""
    cursor = conn.cursor(dictionary=True)
    likeq = f"%{query}%"
    cursor.execute(sql, (likeq,))
    ret = cursor.fetchall()
    if ret:
        return ret
    else:
        return []

def make_user(username, password):
    sql = "insert into user (username) values (%s);"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (username,))
    
    user_id = get_user_id(username)
   
    sql = "insert into user_password (user_id, password_hash) values (%s, %s);"
    cursor.execute(sql, (user_id, make_hash(password)))
    
def verify_user(username, password):
    sql = """select id, password_hash from user u join user_password p on u.id = p.user_id where username=%s"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (username,))
    ret = cursor.fetchone()
    if ret == None:
        return False
    return ret['password_hash'] == make_hash(password)

def make_session(user_id):
    sql = """insert into session (user_id) values (%s)"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (user_id,))
    return cursor.lastrowid

def get_user_id_from_session(session_id):
    sql = """select user_id from session where id = %s"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (session_id,))
    ret = cursor.fetchone()
    if ret:
        return ret["user_id"]
    else:
        return None
    
if __name__ == "__main__":
    #rows = get_movie("Avatar")
    #print(rows)
    print(get_movie("Avatar"))
    
    # insert_review(get_user_id("mrpoopypants"),get_movie("Long Kiss Goodnight")["id"], 7, "Wow I just Love Sammy J!")
    #print(get_movie("Austin Powers in Goldmember"))
    '''print(get_user_reviews(get_user_id("mrpoopypants")))
    print(get_movie_directors(get_movie("Long Kiss Goodnight")["id"]))
    print(get_movie_genres(get_movie("Long Kiss Goodnight")["id"]))
    print(get_movie_production_company(get_movie("Long Kiss Goodnight")["id"]));'''
    
    #insert_follow(get_user_id("mrpoopypants"), get_user_id("yungmarsh"))
    print(get_followers(get_user_id("mrpoopypants")))
    print(get_followees(get_user_id("yungmarsh")))
    print(get_user_id_from_session(4))
    print(get_movie_actors(get_movie("long kiss goodnight")['id']))
    print(get_user_by_id(13))