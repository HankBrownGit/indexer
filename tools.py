import hashlib


def file_hash(file_name):
    hasher = hashlib.md5()
    try:
        with open(file_name, 'rb') as afile:
            buf = afile.read()
            hasher.update(buf)
            digest = hasher.hexdigest()
    except:
        return None
    return digest