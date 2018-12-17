import os
import data_source as ds
import datetime
from data_structures import FileObject
from tools import file_hash


class Crawler(object):

    def __init__(self, entry_point, storage=None, recursive=True, **kwargs):

        if not os.path.isdir(entry_point):
            raise IOError("Entry point not a folder")

        self._entry_point = entry_point
        self._storage = storage
        self._recursive = recursive
        self._verbose = kwargs.get("verbose", False)
        self._filter = kwargs.get("file_type_filter", None)
        if self._filter:
            self._filter = [ex.lower() for ex in self._filter]
        self._ignore_hidden = kwargs.get("ignore_hidden", True)
        self._ignore_symlinks = kwargs.get("ignore_symlinks", True)
        self._min_size = kwargs.get("min_size", None)
        self._max_size = kwargs.get("max_size", None)
        self._total_secs = None

    def start(self):
        start = datetime.datetime.now()
        self.run(self._entry_point)
        self._total_secs = int((datetime.datetime.now() - start).total_seconds())

    def run(self, path):
        try:
            files_folders = os.listdir(path)
        except:
            pass
        else:
            files = []
            folders = []
            for item in files_folders:
                full_path = "/".join([path, item])
                if item.startswith('.') and self._ignore_hidden:
                    continue
                if os.path.islink(full_path) and self._ignore_symlinks:
                    continue
                if os.path.isfile(full_path):
                    if self._filter:
                        name = item[1:] if item.startswith('.') else item
                        if '.' not in name:
                            continue
                        if name.split('.')[1].lower() not in self._filter:
                            continue
                    if self._min_size:
                        if os.path.getsize(full_path) / (1024 * 1024) < self._min_size:
                            continue
                    if self._max_size:
                        if os.path.getsize(full_path) / (1024 * 1024) > self._max_size:
                            continue

                    # Create FileObject with hashed values and timestamp
                    files.append(FileObject(path, item, os.path.getsize(full_path),
                                            file_hash(full_path),
                                            str(datetime.datetime.now())))

                if os.path.isdir(full_path):
                    folders.append(item)

            # Send file list to storage
            if self._storage and len(files):
                self._storage.put({path: files})

            # Walk deeper with recursion
            if self._recursive:
                [self.run(path) for path in [path + "/" + f for f in folders]]

    @property
    def storage(self):
        return self._storage

    @property
    def time(self):
        return self._total_secs


class Redeemer(object):
    def __init__(self, storage, **kwargs):
        self._storage = storage
        self._total_secs = None

    def start(self):
        start = datetime.datetime.now()
        self.run()
        self._total_secs = int((datetime.datetime.now() - start).total_seconds())

    def run(self):
        for row in self._storage.get():
            if not os.path.isfile('/'.join([row['path'], row['file_name']])):
                self._storage.delete(row)
            else:
                if not file_hash('/'.join([row['path'], row['file_name']])) == \
                       row['file_hash']:
                    self._storage.delete(row)


if __name__ == "__main__":
    p = Crawler("/Users/hendrik", file_type_filter=["jpg", "JPG", "txt"],
                storage=ds.DSFactory.build_ds("sqlite", path="/Users/hendrik/test.db"))
    p.start()
    print(p.storage.total_file_count)
    print(p.storage.size)
    print(p.time)
    print(len(p.storage.file_list))
    s = Redeemer(p.storage)
    s.start()

    # p.storage.dump("/Users/Fabebem/t.txt")
    #
    # t = ds.DSFactory.build_ds("dict", load_from="/Users/Fabebem/t.txt")