import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from staging tables into the target tables with the help COPY statements"""
    for query in copy_table_queries:
        # Execute COPY statement to load data from staging table to the target
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into the fact table and dimension tables with the help of INSERT INTO SQL statements"""
    for query in insert_table_queries:
        # Execute INSERT INTO statement to insert data into target tables
        cur.execute(query)
        # Commit changes to the db
        conn.commit()


def main():
    
    """ETL process to connect to the database, loading staging tables, and insert data into target tables  """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connection to the db
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data from staging tables
    load_staging_tables(cur, conn)
    
    #Insert data into target tables
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()