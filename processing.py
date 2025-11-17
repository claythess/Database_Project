import mysql.connector
from pprint import pprint

def connect_db():
    return mysql.connector.connect(
        host="192.168.1.224",
        user="root",
        password="password",
        database="moviedb"
    )

def query_json(conn):
    sql = """
    select cast
    from staging_imdb
    limit 1
    """
    cursor = conn.cursor(dictionary=True)
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