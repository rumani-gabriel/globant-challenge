import sqlite3
"""we configure the rules and structure of the database"""
DATABASE_NAME = "globant.db"

def get_db():
    conn = sqlite3.connect(DATABASE_NAME)
    return conn

def create_job():
    tables = [
        """CREATE TABLE IF NOT EXISTS job(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job TEXT NOT NULL,  
            )
            """
    ]
    db = get_db()
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)

def create_department():
    tables = [
        """CREATE TABLE IF NOT EXISTS job(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                department TEXT NOT NULL,
            )
            """
    ]
    db = get_db()
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)

def create_employees():
    tables = [
        """CREATE TABLE IF NOT EXISTS hired_employees(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                datetime TEXT NOT NULL,
                department_id INTEGER NOT NULL,
                job_id INTEGER NOT NULL
            )
            """
    ]
    db = get_db()
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)