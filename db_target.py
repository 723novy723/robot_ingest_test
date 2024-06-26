import sqlite3
from datetime import datetime
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_company(conn, company):
    """
    Create a new company into the company table
    :param conn:
    :param company:
    :return: company id
    """
    sql = ''' INSERT INTO COMPANY(ID,NAME,AGE,ADDRESS,SALARY)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, company)
    conn.commit()
    return cur.lastrowid


def create_company_ansi(conn, company):
    """
    Create a new company into the company table
    :param conn:
    :param company:
    :return: company id
    """
    sql = ''' INSERT INTO COMPANY_ANSI(NAME,AGE,ADDRESS,SALARY,DOI)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, company)
    conn.commit()
    return cur.lastrowid


def main():
    database = "pythonsqlite_target"

    sql_create_company_table = """CREATE TABLE IF NOT EXISTS COMPANY
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL,
         AGE            INT     NOT NULL,
         ADDRESS        CHAR(50),
         SALARY         REAL);"""

    sql_create_company_ansi_table = """CREATE TABLE IF NOT EXISTS COMPANY_ANSI
         (NAME           TEXT    NOT NULL,
         AGE            INT     NOT NULL,
         ADDRESS        CHAR(50),
         SALARY         REAL,
         DOI            DATETIME NOT NULL);"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create company table
        create_table(conn, sql_create_company_table)

        # create company_ansi table
        create_table(conn, sql_create_company_ansi_table)

        # insert to company
        create_company(conn, (9, 'Olive', 62, 'Toronto', 95000.00))
        create_company(conn, (8, 'Becky', 50, 'Detroit', 55000.00))
        create_company(conn, (7, 'Vince', 45, 'Cleveland', 95000.00))
        create_company(conn, (6, 'Brighton', 15, 'Washington', 55000.00))
        create_company(conn, (5, 'Benny', 18, 'Pennsylvania', 45000.00))
        create_company(conn, (4, 'Mark', 25, 'Rich-Mond', 65000.00))
        create_company(conn, (3, 'Teddy', 23, 'Norway', 20000.00))
        create_company(conn, (2, 'Allen', 25, 'Texas', 15000.00))
        create_company(conn, (1, 'Paul', 32, 'California', 20000.00))

        # insert to company_ansi
        create_company_ansi(conn, ('Olive', 62, 'Toronto', 95000.00, datetime.now()))
        create_company_ansi(conn, ('Becky', 50, 'Detroit', 55000.00, datetime.now()))
        create_company_ansi(conn, ('Vince', 45, 'Cleveland', 95000.00, datetime.now()))
        create_company_ansi(conn, ('Brighton', 15, 'Washington', 55000.00, datetime.now()))
        create_company_ansi(conn, ('Benny', 18, 'Pennsylvania', 45000.00, datetime.now()))
        create_company_ansi(conn, ('Mark', 25, 'Rich-Mond', 65000.00, datetime.now()))
        create_company_ansi(conn, ('Teddy', 23, 'Norway', 20000.00, datetime.now()))
        create_company_ansi(conn, ('Allen', 25, 'Texas', 15000.00, datetime.now()))
        create_company_ansi(conn, ('Paul', 32, 'California', 20000.00, datetime.now()))

    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
