import os, sys
import importlib

from cloudant.client import Cloudant
cc = Cloudant(os.environ['COUCHUSER'], os.environ['COUCHPASS'], url="http://localhost:5984", connect=True)

all_dbs = cc.all_dbs()
couch_docs = {}

class CouchFinder(importlib.abc.MetaPathFinder):
    def find_spec(fullname, path, target=None):
        if not fullname in couch_docs:
            name_array = fullname.split('.')
            if name_array[0] in all_dbs:
                db = cc[name_array[0]]
                doc_id = 'module:' + '.'.join(name_array[1:])
                if doc_id in db:
                    couch_docs[fullname] = db[doc_id]

        if fullname in couch_docs:
            return importlib.machinery.ModuleSpec(fullname, CouchLoader())

        return None

    def invalidate_caches():
        all_dbs = cc.all_dbs()
        couch_docs = {}

class CouchLoader(importlib.abc.SourceLoader):
    def get_filename(self, fullname):
        return 'x-couchloader:%s' % fullname

    def get_data(self, path):
        protocol, fullname = path.split(':')
        return couch_docs[fullname]['source']

sys.meta_path.append(CouchFinder)
