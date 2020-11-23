from loadFile.loadCsv import insetCsvToRedis
import redis


class RedisDB:

    def __init__(self, host, db_idx):
        self.host = host
        self.db_idx = db_idx
        self.r = redis.Redis(self.host, decode_responses=True, db=self.db_idx)

    def insert(self, key, value):
        self.r.set(key, value)

    def insertFromCsv(self, filename):
        insetCsvToRedis(self.r, filename)

    def getValue(self, key):
        return self.r.get(key)

    def save(self):
        self.r.save()

    def getAllKeys(self):
        return self.r.keys()

    def searchKeyInDB(self, business_name: str):
        keys = self.getAllKeys()
        for key in keys:
            if business_name.find(key) != -1:
                return self.getValue(key)
        return None

    def deleteAll(self):
        self.r.flushall()

    def close(self):
        self.r.close()

# def searchKeyInDB(redis_db: RedisDB, business_name: str):
#     keys = redis_db.getAllKeys()
#     for key in keys:
#         if business_name.find(key) != -1:
#             return redis_db.getValue(key)
#     return None

    # def search(self, str):
    #     keys = self.get_all_keys()
    #     for key in keys:
    #         if str.find(key) != -1:
    #             return self.db[key]
    #     return None


# if __name__ == '__main__':
    # db_obj = RedisDB('localhost', 0)
    # db_obj.insertFromCsv('db_csv.csv')
    # db_obj.save()
    # print(db_obj.getAllKeys())
    # res = searchKeyInDB(db_obj, "פז תחנת דלק")
    # print(res)


