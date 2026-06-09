import sqlite3
from pprint import pprint

def connect_db():
    con = sqlite3.connect('movie.db')
    con.row_factory = sqlite3.Row  
    return con

def query_json(conn):
    sql = """
    select cast
    from staging_imdb
    limit 1
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def main():
    conn = connect_db()
    for row in query_json(conn):
        pprint(row)
    conn.close()

if __name__ == "__main__":
    main()