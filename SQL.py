import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except sqlite3.Error as e:
       print(e)
   return conn

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)   

def add_author(conn, author):
   sql = '''INSERT INTO author(name, birth_date, death_date)
             VALUES(?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, author)
   conn.commit()
   return cur.lastrowid

def add_book(conn, book):
   sql = '''INSERT INTO book(author_id, name, genre, release_date)
             VALUES(?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, book)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == "__main__":
   
   create_author_sql = """
   -- author table
   CREATE TABLE IF NOT EXISTS author (
      id integer PRIMARY KEY,
      name text NOT NULL,
      birth_date text,
      death_date text
   );
   """

   create_book_sql = """
   -- book table
   CREATE TABLE IF NOT EXISTS book (
      id integer PRIMARY KEY,
      author_id integer NOT NULL,
      name VARCHAR(250) NOT NULL,
      genre TEXT,
      release_date text NOT NULL,
      FOREIGN KEY (author_id) REFERENCES author (id)
   );
   """

db_file = "database.db"

conn = create_connection(db_file)
if conn is not None:
       execute_sql(conn, create_author_sql)
       execute_sql(conn, create_book_sql)
       conn.close()


author = ("Gombrowicz", "2020-05-11 00:00:00", "2020-05-13 00:00:00")
conn = create_connection("database.db")
author_id = add_author(conn, author)

book = (
      author_id,
      "Ferdydurke",
      "Novel",
      "2020-05-11 15:00:00"
)

book_id = add_book(conn, book)

print(author, book)
conn.commit()