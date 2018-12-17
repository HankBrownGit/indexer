
sql_schema = [["path", "TEXT"],
              ["file_name", "TEXT"],
              ["hash", "TEXT"],
              ["timestamp", "TEXT"]]

flat_sql_schema = [e[0] for e in sql_schema]


class FileObject(object):
    def __init__(self, path, name, file_hash, timestamp):
        self.path = path
        self.name = name
        self.file_hash = file_hash
        self.timestamp = timestamp
