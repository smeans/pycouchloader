import os, sys
import datetime
import uuid
import importlib
import requests
import argparse
import glob

import logging
logging.basicConfig(format="%(asctime)s: %(levelname)s: %(message)s")

import pyinotify

from cloudant.client import Cloudant

cc = None
all_dbs = None
couch_docs = {}

def connect(url):
    try:
        global cc, all_dbs
        cc = Cloudant(os.environ['COUCHUSER'], os.environ['COUCHPASS'], url=url, connect=True)
        all_dbs = cc.all_dbs()
    except KeyError:
        logging.error('please configure the COUCHUSER and COUCHPASS environment variables')
        exit(1)
    except Exception as e:
        logging.error('unable to connect to CouchdB', e)
        exit(1)

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
        return couch_docs['pycode:%s' % fullname]['current']['source']

sys.meta_path.append(CouchFinder)

def push_version(db, fn):
    path = os.path.dirname(fn)
    base, ext = os.path.splitext(os.path.basename(fn))

    with open(fn, 'r') as f:
        cn = '%s.%s' % (path.replace('/', '.'), base)
        doc_id = 'pycode:%s' % cn

        if not doc_id in db:
            db.create_document({
                '_id': doc_id,
                'current': None,
                'history': []
            })

        sd = db[doc_id]
        source = f.read()

        if sd['current']:
            if sd['current']['source'] == source:
                return

            sd['history'].insert(0, sd['current'])
            logging.info('archiving %s version %s' % (cn, sd['current']['push_id']))

        sd['current'] = {
            'push_ts': datetime.datetime.utcnow().isoformat(),
            'push_id': str(uuid.uuid4()),
            'source': source
        }

        sd.save()

def sync_folder(db, path):
    logging.debug('%s: syncing folder %s' % (db.metadata()['db_name'], path))

    for n in os.listdir(path):
        if n[0] == '.':
            continue

        fn = os.path.join(path, n)

        if os.path.isdir(fn):
            sync_folder(db, fn)
        elif os.path.isfile(fn) and fn.endswith('.py'):
            push_version(db, fn)

def handle_sync(args):
    for n in os.listdir(args.sync):
        if os.path.isdir(n):
            db_name = '%s_%s' % (args.dbprefix, n)

            if db_name in all_dbs:
                logging.info('syncing package %s' % n)
                sync_folder(cc[db_name], n)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CouchDB Python class loader.')
    parser.add_argument('class_name', nargs='*', help='class names to be loaded')
    parser.add_argument('--dbprefix', default='pyc', help='code database prefix')
    parser.add_argument('--debug', action='store_const', const=True, help='enable debug logging')
    parser.add_argument('--url', default='http://localhost:5984', help='target CouchDB URL')
    parser.add_argument('--sync')
    args = parser.parse_args()

    connect(args.url)

    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)

    if args.sync:
        handle_sync(args)
    else:
        for cn in args.class_name:
            importlib.import_module(cn)
else:
    connect("http://localhost:5984")
