import config
import os
from loadFile.load_xl import readExcelFile
from database.redis.myConnRedis import business_keys_db, business_names_db

project_root = os.path.dirname(os.path.dirname(__file__))
path_to_business_name = os.path.join(project_root, 'input', 'BusinessNamesDB.csv')
path_to_business_keys = os.path.join(project_root, 'input', 'BusinessKeysDB.csv')
path_to_redis_server = config.path_to_redis_server  # change to your path


def insertFromCsv():
    business_names_db.insertFromCsv(path_to_business_name)
    business_keys_db.insertFromCsv(path_to_business_keys)
    business_names_db.save()


def createConn():
    os.startfile(path_to_redis_server)


def upload_xl_from_zero(filename, user_id):
    createConn()
    insertFromCsv()
    readExcelFile(filename, user_id)


def upload_xl(filename, user_id):
    createConn()
    readExcelFile(filename, user_id)
