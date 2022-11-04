#!/usr/bin/python3
# Python 3.10
import logging
import os
import sys
import time
from configparser import ConfigParser
import psycopg2
import pymongo
from pymongo import database
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


'''
Query to count all available films
'''
count_films = [{'$count': "film"}]


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
            logging.info(f'Command used to insert: collection.insert_many(dicttables.get(table))')
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

    def create_view(self, name: str, viewon: str, pipeline: list) -> None:
        try:
            self.db: pymongo.database.Database
            collection = self.db.create_collection(
                name,
                viewOn=viewon,
                pipeline=pipeline)

        except:
            logging.error("Unable to Create customer_list view.")
            logging.error(f'{sys.exc_info()[1]}')
            logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

    def tranform(self, collection: str, pipeline: list) -> None:
        try:
            logging.info(f'Transform Collection: {collection}')
            logging.info(f'Pipeline used: {pipeline}')
            if collection != '' and isinstance(pipeline, list):
                result = self.db[collection].aggregate(pipeline)
                logging.info(f'Transformation returned:')
                for line in result:
                    logging.info(f'{line}')

        except:
            logging.error("Unable to run query.")
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
        logging.info(f'######### CREATE START ############################')
        MongoDB.json_dict_insert(json_tables)
        logging.info(f'######### READ START ############################')
        logging.info(f'--------Number of available films')
        MongoDB.tranform('film', count_films)
        logging.info(f'--------Number of films per location')

        MongoDB.create_view('customer_list', 'customer', get_pipeline_customer_view())
    except Exception as e:
        logging.error(e.__class__)
        logging.error(f'{sys.exc_info()[1]}')
        logging.error(f'Error on line {sys.exc_info()[-1].tb_lineno}')

    finally:
        if PostgresDB.conn is not None:
            PostgresDB.conn.close()


def get_pipeline_customer_view() -> list:
    """
        Returns the Pipeline to reproduce the behavior of view "customer_list"

         Methods:
        -------
        1. Join Collection address by address_id
        2. unwind address to get rid of internal lists
        3. Join Collection city by city_id
        4. unwind city to get rid of internal lists
        5. Join Collection country by country_id
        6. unwind country to get rid of internal lists
        7. Concat first name and last name to a new field fullName
        8. evaluate the field activebool and write active values to new field notes
        9. Combine all necessary fields to view
        """
    return [{'$lookup': {
        'from': "address",
        'localField': "address_id",
        'foreignField': "address_id",
        'as': 'address'
    }},
        {'$unwind': {
            'path': "$address",
            'preserveNullAndEmptyArrays': True
        }},

        {'$lookup': {
            'from': "city",
            'localField': "address.city_id",
            'foreignField': "city_id",
            'as': "city"
        }},
        {'$unwind': {
            'path': "$city",
            'preserveNullAndEmptyArrays': True
        }},

        {'$lookup': {
            'from': "country",
            'localField': "city.country_id",
            'foreignField': "country_id",
            'as': "country"
        }},
        {'$unwind': {
            'path': "$country",
            'preserveNullAndEmptyArrays': True
        }},

        {'$addFields': {
            'fullName': {'$concat': ['$first_name', ' ', '$last_name']}
        }},

        {'$addFields': {
            'notes': {'$cond': {'if': "$activebool", 'then': "active", 'else': ""}}
        }},

        {'$project': {
            '_id': 1,
            'customer_id': 1,
            'name': "$fullName",
            'address': "$address.address",
            'zip code': "$address.postal_code",
            'phone': "$address.phone",
            'city': "$city.city",
            'country': "$country.country",
            'notes': "$notes",
            'sid': "$store_id"
        }}

    ]


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
            logging.FileHandler(filename=app_dir('output.log'), mode='w', encoding='utf-8'),
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
