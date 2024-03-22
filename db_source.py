import sqlite3
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


def main():
    database = "pythonsqlite_source"

    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    priority integer,
                                    status_id integer NOT NULL,
                                    project_id integer NOT NULL,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    FOREIGN KEY (project_id) REFERENCES projects (id)
                                );"""

    sql_create_company_table = """CREATE TABLE IF NOT EXISTS COMPANY
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL,
         AGE            INT     NOT NULL,
         ADDRESS        CHAR(50),
         SALARY         REAL);"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_projects_table)

        # create tasks table
        create_table(conn, sql_create_tasks_table)

        # create company table
        create_table(conn, sql_create_company_table)

        # insert to company
        create_company(conn, (1, 'Paul', 32, 'California', 20000.00))
        create_company(conn, (2, 'Allen', 25, 'Texas', 15000.00))
        create_company(conn, (3, 'Teddy', 23, 'Norway', 20000.00))
        create_company(conn, (4, 'Mark', 25, 'Rich-Mond', 65000.00))
        create_company(conn, (5, 'Benny', 18, 'Pennsylvania', 45000.00))
        create_company(conn, (6, 'Brighton', 15, 'Washington', 55000.00))
        create_company(conn, (7, 'Vince', 45, 'Cleveland', 95000.00))
        create_company(conn, (8, 'Becky', 50, 'Detroit', 55000.00))
        create_company(conn, (9, 'Olive', 62, 'Toronto', 95000.00))
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
