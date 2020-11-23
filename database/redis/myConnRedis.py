
from database.redis.RedisDB import RedisDB

business_names_db = RedisDB('localhost', 1)
business_keys_db = RedisDB('localhost', 0)