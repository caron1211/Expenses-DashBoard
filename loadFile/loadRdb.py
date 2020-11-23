import csv
import os
from rdbtools import RdbCallback, RdbParser
import pandas as pd

from rdbtools.callbacks import JSONCallback


def load_rdb(filename, filters=None):
    r = MockRedis()
    parser = RdbParser(r, filters)
    parser.parse(filename)
    return r


class MockRedis(RdbCallback):
    def __init__(self):
        super(MockRedis, self).__init__(string_escape=True)
        self.databases = {}
        self.lengths = {}
        self.expiry = {}
        self.methods_called = []
        self.dbnum = 0

    def currentdb(self):
        return self.databases[self.dbnum]

    def store_expiry(self, key, expiry):
        self.expiry[self.dbnum][key] = expiry

    def store_length(self, key, length):
        if not self.dbnum in self.lengths:
            self.lengths[self.dbnum] = {}
        self.lengths[self.dbnum][key] = length

    def get_length(self, key):
        if not key in self.lengths[self.dbnum]:
            raise Exception('Key %s does not have a length' % key)
        return self.lengths[self.dbnum][key]

    def start_rdb(self):
        self.methods_called.append('start_rdb')

    def start_database(self, dbnum):
        self.dbnum = dbnum
        self.databases[dbnum] = {}
        self.expiry[dbnum] = {}
        self.lengths[dbnum] = {}

    def set(self, key, value, expiry, info):
        self.currentdb()[key] = value
        if expiry:
            self.store_expiry(key, expiry)

    def start_hash(self, key, length, expiry, info):
        if key in self.currentdb():
            raise Exception('start_hash called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = {}
        if expiry:
            self.store_expiry(key, expiry)
        self.store_length(key, length)

    def hset(self, key, field, value):
        if not key in self.currentdb():
            raise Exception('start_hash not called for key = %s', key)
        self.currentdb()[key][field] = value

    def end_hash(self, key):
        if not key in self.currentdb():
            raise Exception('start_hash not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key]:
            raise Exception('Lengths mismatch on hash %s, expected length = %d, actual = %d'
                            % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))

    def start_set(self, key, cardinality, expiry, info):
        if key in self.currentdb():
            raise Exception('start_set called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = []
        if expiry:
            self.store_expiry(key, expiry)
        self.store_length(key, cardinality)

    def sadd(self, key, member):
        if not key in self.currentdb():
            raise Exception('start_set not called for key = %s', key)
        self.currentdb()[key].append(member)

    def end_set(self, key):
        if not key in self.currentdb():
            raise Exception('start_set not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key]:
            raise Exception('Lengths mismatch on set %s, expected length = %d, actual = %d'
                            % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))

    def start_list(self, key, expiry, info):
        if key in self.currentdb():
            raise Exception('start_list called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = []
        if expiry:
            self.store_expiry(key, expiry)

    def rpush(self, key, value):
        if not key in self.currentdb():
            raise Exception('start_list not called for key = %s', key)
        self.currentdb()[key].append(value)

    def end_list(self, key, info):
        if not key in self.currentdb():
            raise Exception('start_set not called for key = %s', key)
        self.store_length(key, len(self.currentdb()[key]))

    def start_sorted_set(self, key, length, expiry, info):
        if key in self.currentdb():
            raise Exception('start_sorted_set called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = {}
        if expiry:
            self.store_expiry(key, expiry)
        self.store_length(key, length)

    def zadd(self, key, score, member):
        if not key in self.currentdb():
            raise Exception('start_sorted_set not called for key = %s', key)
        self.currentdb()[key][member] = score

    def end_sorted_set(self, key):
        if not key in self.currentdb():
            raise Exception('start_set not called for key = %s', key)
        if len(self.currentdb()[key]) != self.lengths[self.dbnum][key]:
            raise Exception('Lengths mismatch on sortedset %s, expected length = %d, actual = %d'
                            % (key, self.lengths[self.dbnum][key], len(self.currentdb()[key])))

    def start_module(self, key, module_name, expiry, info):
        if key in self.currentdb():
            raise Exception('start_module called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = {'module_name': module_name}
        if expiry:
            self.store_expiry(key, expiry)
        return False

    def end_module(self, key, buffer_size, buffer=None):
        if not key in self.currentdb():
            raise Exception('start_module not called for key = %s', key)
        self.store_length(key, buffer_size)
        pass

    def start_stream(self, key, listpacks_count, expiry, info):
        if key in self.currentdb():
            raise Exception('start_stream called with key %s that already exists' % key)
        else:
            self.currentdb()[key] = {}
        if expiry:
            self.store_expiry(key, expiry)
        pass

    def stream_listpack(self, key, entry_id, data):
        if not key in self.currentdb():
            raise Exception('start_hash not called for key = %s', key)
        self.currentdb()[key][entry_id] = data
        pass

    def end_stream(self, key, items, last_entry_id, cgroups):
        if not key in self.currentdb():
            raise Exception('start_stream not called for key = %s', key)
        self.store_length(key, items)

    def end_database(self, dbnum):
        if self.dbnum != dbnum:
            raise Exception('start_database called with %d, but end_database called %d instead' % (self.dbnum, dbnum))

    def end_rdb(self):
        self.methods_called.append('end_rdb')


def load_rdb_db(filename):
    r = load_rdb(filename)  # return MockRedis
    db = r.databases[1]  # dict
    return db


if __name__ == '__main__':
    filename = "dump.rdb"
    res = load_rdb_db(filename)
    unidict = {k.decode('utf8'): v.decode('utf8') for k, v in res.items()}

    csv_file = "Names.csv"
    csv_columns = ['key', 'value']

    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for key, value in unidict.items():
                thisdict = {
                    "key": key,
                    "value": value
                }
                writer.writerow(thisdict)
    except IOError:
        print("I/O error")

    for key, value in unidict.items():
        print("key: {} , value: {}".format(key,value))
