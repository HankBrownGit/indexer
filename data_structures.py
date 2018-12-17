
sql_schema = [["path", "TEXT"],
              ["file_name", "TEXT"],
              ["size_bytes", "INT"],
              ["file_hash", "TEXT"],
              ["access_timestamp", "TEXT"]]

flat_sql_schema = [e[0] for e in sql_schema]


class FileObject(object):
    def __init__(self, path, file_name, size_bytes, file_hash,
                 access_timestamp):
        self.path = path
        self.file_name = file_name
        self.size_bytes = size_bytes
        self.file_hash = file_hash
        self.access_timestamp = access_timestamp
