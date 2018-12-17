from collections import defaultdict
import sqlite3
from data_structures import sql_schema, flat_sql_schema
import pickle
from sys import getsizeof


class DataStorage(object):

    @property
    def DS_type(cls):
        return cls._DS_type

    @property
    def metadata(self):
        return self._metadata

    def put(self, data):
        pass

    def find(self, key):
        pass

    def close(self):
        pass


class Sqlite(DataStorage):

    def __init__(self, path):
        self._path = path
        try:
            self._conn = sqlite3.connect(path)
        except ConnectionError:
            raise ConnectionError('Error connecting to DB ' + path)

        create_string = "create table if not exists CrawlerTable (" \
                        + ', '.join([t[0] + " "
                        + t[1] for t in sql_schema]) \
                        + ", UNIQUE(path, file_name, file_hash))"
        self._conn.execute(create_string)
        self._cur = self._conn.cursor()

    def put(self, data):
        values = []
        for key, row in data.items():
            for file_object in row:
                if self._check_validity(file_object):
                    values.append("('{}', '{}', '{}', '{}', '{}')".format
                                  (file_object.path,
                                   file_object.file_name,
                                   file_object.size_bytes,
                                   file_object.file_hash,
                                   file_object.access_timestamp))
        if len(values):
            # replace w/
            # c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)
            sql_string = "INSERT OR IGNORE INTO CrawlerTable (path, " \
                         "file_name, size_bytes, file_hash, access_timestamp) " \
                         "VALUES " + ', '.join(values)
            self._cur.execute(sql_string)
        self._conn.commit()

    @staticmethod
    def _check_validity(file_object):
        # Check if ' or " in path. Those characters are not saved correctly in
        # the db an the rows are hence dismissed.

        if True in ["'" in t or '"' in t for t in [file_object.path,
                                                   file_object.file_name,
                                                   file_object.file_hash,
                                                   file_object.access_timestamp]]:
            return False
        return True

    def get(self):
        for row in self._cur.execute('SELECT "{}" FROM CrawlerTable'.
                format('", "'.join(flat_sql_schema))):
            yield dict({k[0]: v for (k, v) in zip(sql_schema, row)})

    def delete(self, row):
        self._cur.execute("DELETE FROM CrawlerTable WHERE {}".format(
        ' AND '.join([k + "='" + str(v) + "'" for k, v in row.items()])))
        self._conn.commit()

    def close(self):
        self._conn.close()

    def find(self, key):
        pass

    @property
    def items(self):
        return 0

    @property
    def size(self):
        return 0

    @property
    def total_file_count(self):
        return 0

    @property
    def file_list(self):
        self._cur.execute("SELECT * FROM CrawlerTable")
        return self._cur.fetchall()


class DictStorage(DataStorage):

    def __init__(self, **kwargs):
        self._DS_type = "dict"
        self._metadata = None
        self._dict = defaultdict(list)

    def put(self, data):
        self._dict.update(data)

    def find(self, key):
        return self._dict.get(key, None)

    @property
    def items(self):
        return self._dict

    @property
    def size(self):
        return getsizeof(self._dict) / (1024 * 1024)

    @property
    def total_file_count(self):
        return len([item for sublist in [file for file in [files for files in self._dict.values()]] for item in sublist])

    @property
    def file_list(self):
        return [item for sublist in [t for t in [files for files in self._dict.values()]] for item in sublist]

    def dump(self, path):
        with open(path, 'wb') as outfile:
            pickle.dump(self._dict, outfile)

    @classmethod
    def from_file(cls, file):
        with open(file, 'rb') as infile:
            load_dict = pickle.load(infile)

        return_class = DictStorage()
        return_class._dict = load_dict
        return return_class


class DSFactory(object):

    @classmethod
    def build_ds(cls, ds_type, **kwargs):
        if ds_type == "dict":
            load_file = kwargs.get("load_from", None)
            if load_file:
                return DictStorage.from_file(load_file)
            return DictStorage()
        if ds_type == "sqlite":
            if kwargs.get("path"):
                return Sqlite(kwargs.get("path"))
            else:
                raise IOError("Sqlite data storage requires path variable")
