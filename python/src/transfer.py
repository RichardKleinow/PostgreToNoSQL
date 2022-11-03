#!/usr/bin/python3
# Python 3.10
import logging
import os
import sys
import time
from configparser import ConfigParser
import psycopg2
import pymongo
# Benchmarking
from profilehooks import timecall

class PGDB:
    """
    Handler for Connection to Postgre Database

    Methods:
    -------
    list_tables
        internal method to create a list of all available tables
    get_json_tables -> dict
        create a dict {tablename:[tablerows as json]} for all available tables
    """

    def __init__(self, param: dict):
        try:
            # Init for later usage
            self.listTables = []
            # Create Connection + Cursor
            self.conn = psycopg2.connect(**param)
            self.cursor = self.conn.cursor()
            # Check Connection
            self.cursor.execute('SELECT version()')
            logging.info(f'PostgresVersion: {self.cursor.fetchone()}')
            # Get available tables
            self.list_tables()

        except(Exception, psycopg2.DatabaseError) as e:
            logging.error(e)
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

    def list_tables(self) -> None:
        sql = '''
        SELECT tablename from pg_catalog.pg_tables 
        WHERE schemaname != 'pg_catalog'
        AND schemaname != 'information_schema'
        '''
        self.cursor.execute(sql)
        listTables = self.cursor.fetchall()
        self.listTables = [table for t in listTables for table in t if table is not None]
        logging.info(f'{len(listTables)} tables found in database')

    def get_json_tables(self) -> dict:
        dict_json_tables = {}
        try:
            for table in self.listTables:
                sql = f'''
                SELECT json_agg({table}.*) FROM {table}
                '''
                self.cursor.execute(sql)
                json_table = self.cursor.fetchall()
                if json_table is not None:
                    dict_json_tables[table] = [k for i in json_table for j in i for k in j if k is not None]

        except Exception as e:
            logging.error(e.__class__)
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

        finally:
            logging.info(f'Transformation of {len(dict_json_tables)} tables successful')
            return dict_json_tables


class MDB:
    """
    Handler for Connection to Mongo-DB database

     Methods:
    -------
    json_dict_insert
        takes dict of json tables and import them as collections
    """

    def __init__(self, param: dict):
        try:
            # Init for later usage
            self.listCollections = []
            # Create Connection
            self.client = pymongo.MongoClient(**param, serverSelectionTimeoutMS=5000)
            # Clear potentially old and create new db
            self.client.drop_database('dvdrental')
            self.db = self.client.dvdrental
            # Check Connection
            info = self.client.server_info()
            logging.info(f'Mongo-DB Server version: {info.get("version")}')

        except (Exception, pymongo.mongo_client.ServerSelectionTimeoutError) as e:
            logging.error(e.__class__)
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

    def json_dict_insert(self, dicttables: dict) -> None:
        try:
            logging.info(f'Starting to insert {len(dicttables)} json elements into new database.')
            for table in dicttables:
                # Create Collection
                collection = self.db[table]
                collection: pymongo.collection.Collection
                # Bulk insert the prepared json data
                ids = collection.insert_many(dicttables.get(table))
                # Save Collection
                self.listCollections.append(collection)
            logging.info(f'{len(self.listCollections)} tables were successfully inserted as collections.')

        except:
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

@timecall
def main():
    dictPgDB = read_config('database.ini', 'postgresql')
    PostgresDB = PGDB(dictPgDB)
    dictMongoDB = read_config('database.ini', 'mongodb')
    MongoDB = MDB(dictMongoDB)

    try:
        json_tables = PostgresDB.get_json_tables()
        MongoDB.json_dict_insert(json_tables)
    except Exception as e:
        logging.error(e.__class__)
        logging.error(f'{sys.exc_info()[1]}')
        logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

    finally:
        if PostgresDB.conn is not None:
            PostgresDB.conn.close()


def read_config(filename='database.ini', section='postgresql') -> dict:
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        for param in parser.items(section):
            value = param[1]
            try:
                value = int(param[1], 10)
            except(Exception, ValueError):
                pass
            db[param[0]] = value
    else:
        logging.error(f'Section {section} not found in the {filename} file')
        raise FileNotFoundError

    return db


def init_logging():
    log_format = f"%(asctime)s [%(processName)s] [%(name)s] [%(levelname)s] %(message)s"
    # logging.getLogger('').disabled = True
    log_level = logging.DEBUG
    # noinspection PyArgumentList
    logging.basicConfig(
        format=log_format,
        level=log_level,
        force=True,
        handlers=[
            logging.FileHandler(filename=app_dir('debug.log'), mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def app_dir(relative_path='') -> os.path:
    if getattr(sys, 'frozen', False):
        path = os.path.dirname(sys.executable)
    else:  # called from pycharm or direct with python
        path = os.path.dirname(__file__)
    return os.path.join(path, relative_path)


def home_dir(relative_path='') -> os.path:
    if getattr(sys, 'frozen', False):
        path = sys._MEIPASS
    else:  # called from pycharm or direct with python
        path = os.path.dirname(__file__)
    return os.path.join(path, relative_path)


if __name__ == "__main__":
    init_logging()
    main()
