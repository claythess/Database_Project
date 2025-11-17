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

def get_movie(title, year = None) -> dict:
    if year:
        sql = """
        select * from movie where title = %s and year = %d limit 1;
        """
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, (title, year))
    else:
        sql = """
        select * from movie where title like %s limit 1;
        """
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, (title,))
        
    ret = cursor.fetchone()
    if ret:
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
        imdbRating = movie_data["IMDBRating"]
        
        sql = """insert into movie (title, language, image_url, imdbRating, year) values (%s, %s, %s, nullif(%s,"N/A"), %s)"""
        
        movie_id = cursor.execute(sql, (title, language, image_url, imdbRating, year,))
        
        
        
        

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


def make_user(username, password):
    sql = "insert into user (username) values (%s);"
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, (username,))
    
    user_id = get_user_id(username)['id']
   
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
    
    
if __name__ == "__main__":
    rows = get_movie("Avatar")
    print(rows)
    
    get_movie("long kiss goodnight")