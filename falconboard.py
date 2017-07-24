import json
import logging
import uuid
from wsgiref import simple_server

import falcon
import pymongo


class StorageEngine:
    client = pymongo.MongoClient('mongodb://falconboard:password@ds161022.mlab.com:61022/falconboard')
    db = client.get_default_database()

    def get_threads(self, board_id):
        if not board_id in ['b']:
            raise falcon.HTTPError(falcon.HTTP_404, 'Unknown board', 'Board not found')
        coll = self.db['board.' + board_id]
        threads = []
        for thread in coll.find({"is_thread": True}):
            comments = coll.find({"parent_id": thread["_id"]}).skip(coll.count() - 3)
            threads = threads + [thread] + list(comments)
        return threads

    pass


class StorageError(Exception):
    @staticmethod
    def handle(ex, req, resp, params):
        description = ('Sorry, couldn\'t write your thread to the '
                       'database. It worked on my box.')

        raise falcon.HTTPError(falcon.HTTP_725,
                               'Database Error',
                               description)


def token_is_valid(token, board_id):
    return True  # Suuuuuure it's valid...


def auth(req, resp, params):
    # Alternatively, do this in middleware
    token = req.get_header('X-Auth-Token')

    if token is None:
        token = uuid.uuid4()
        req.headers['X-Auth-Token'] = token


def check_media_type(req, resp, params):
    if not req.client_accepts_json:
        raise falcon.HTTPUnsupportedMediaType(
            'This API only supports the JSON media type.',
            href='http://docs.examples.com/api/json')


class ThreadsResource:

    def __init__(self, db):
        self.db = db
        self.logger = logging.getLogger('threadsapp.' + __name__)

    def on_get(self, req, resp, board_id):
        marker = req.get_param('marker') or ''
        limit = req.get_param_as_int('limit') or 50

        try:
            result = self.db.get_threads(board_id)
        except Exception as ex:
            self.logger.error(ex)

            description = ('Aliens have attacked our base! We will '
                           'be back as soon as we fight them off. '
                           'We appreciate your patience.')

            raise falcon.HTTPServiceUnavailable(
                'Service Outage',
                description,
                30)

        resp.set_header('X-Powered-By', 'Donuts')
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(result)

    def on_post(self, req, resp, board_id):
        try:
            raw_json = req.stream.read()
        except Exception:
            raise falcon.HTTPError(falcon.HTTP_748,
                                   'Read Error',
                                   'Could not read the request body. Must be '
                                   'them ponies again.')

        try:
            thread = json.loads(raw_json, 'utf-8')
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect.')

        proper_thread = self.db.add_thread(thread)

        resp.status = falcon.HTTP_201
        resp.location = '/%s/threads/%s' % (board_id, proper_thread.id)

# Configure your WSGI server to load "threads.app" (app is a WSGI callable)
app = falcon.API(before=[auth, check_media_type])

db = StorageEngine()
threads = ThreadsResource(db)
app.add_route('/{board_id}/threads', threads)

# If a responder ever raised an instance of StorageError, pass control to
# the given handler.
app.add_error_handler(StorageError, StorageError.handle)

# Useful for debugging problems in your API; works with pdb.set_trace()
if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
