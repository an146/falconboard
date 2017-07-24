import json
import logging
import uuid
import traceback
from wsgiref import simple_server

import falcon
import pymongo


logging.basicConfig(level=logging.DEBUG)

def mongo_limit(cursor, limit):
    return cursor.skip(cursor.count() - limit)

class StorageEngine:
    client = pymongo.MongoClient('mongodb://falconboard:password@ds161022.mlab.com:61022/falconboard')
    db = client.get_default_database()

    def check_board(self, board):
        if not board in ['b']:
            raise falcon.HTTPError(falcon.HTTP_404, 'Unknown board', 'Board not found')

    def check_post(self, post):
        for field in post:
            if not field in ['title', 'text', 'parent_id', 'sage']:
                raise falcon.HTTPError(falcon.HTTP_403, 'Invalid argument', 'Unknown field: ' + field)

    def get_posts(self, board):
        self.check_board(board)
        coll = self.db['board.' + board]
        posts = []
        for thread in coll.find({"is_thread": True}):
            comments = mongo_limit(coll.find({"parent_id": thread["_id"]}), 3)
            posts = posts + [thread] + list(comments)
        return posts

    def get_post(self, board, post):
        self.check_board(board)
        coll = self.db['board.' + board]
        post = coll.find_one({"_id": post})
        comments = coll.find({"parent_id": post})
        return [post] + list(comments)

    def add_post(self, board, post):
        self.check_board(board)
        self.check_post(post)
        coll = self.db['board.' + board]
        _id = int(6)
        post["_id"] = _id
        return coll.insert_one(post)



class StorageError(Exception):
    @staticmethod
    def handle(ex, req, resp, params):
        description = ('Sorry, couldn\'t write your thread to the '
                       'database. It worked on my box.')

        raise falcon.HTTPError(falcon.HTTP_725,
                               'Database Error',
                               description)


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


class PostResource:
    def __init__(self, db):
        self.db = db

    def on_get(self, req, resp, board, post):
        try:
            result = self.db.get_post(board, int(post))
        except Exception as ex:
            traceback.print_exc()
            logging.error(ex)

            description = ('Aliens have attacked our base! We will '
                           'be back as soon as we fight them off. '
                           'We appreciate your patience.')

            raise falcon.HTTPServiceUnavailable(
                'Service Outage',
                description,
                30)

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(result)

class ThreadsResource:
    def __init__(self, db):
        self.db = db

    def on_get(self, req, resp, board):
        marker = req.get_param('marker') or ''
        limit = req.get_param_as_int('limit') or 50

        try:
            result = self.db.get_posts(board)
        except Exception as ex:
            logging.error(ex)

            description = ('Aliens have attacked our base! We will '
                           'be back as soon as we fight them off. '
                           'We appreciate your patience.')

            raise falcon.HTTPServiceUnavailable(
                'Service Outage',
                description,
                30)

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(result)

    def on_post(self, req, resp, board):
        try:
            raw_json = req.stream.read()
        except Exception:
            raise falcon.HTTPError(falcon.HTTP_748,
                                   'Read Error',
                                   'Could not read the request body. Must be '
                                   'them ponies again.')

        try:
            post = json.loads(raw_json, 'utf-8')
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect.')

        post = self.db.add_post(board, post)

        resp.status = falcon.HTTP_201
        resp.location = '/%s/%s/' % (board, post.inserted_id)

# Configure your WSGI server to load "threads.app" (app is a WSGI callable)
app = falcon.API(before=[auth, check_media_type])

db = StorageEngine()

app.add_route('/{board}/', ThreadsResource(db))
app.add_route('/{board/{post}/', PostResource(db))

# If a responder ever raised an instance of StorageError, pass control to
# the given handler.
app.add_error_handler(StorageError, StorageError.handle)

# Useful for debugging problems in your API; works with pdb.set_trace()
if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
