import json
import logging
import uuid
import traceback
from wsgiref import simple_server

import falcon
import falcon_auth
import pymongo


logging.basicConfig(level=logging.DEBUG)

def mongo_limit(cursor, limit):
    skip = cursor.count() - limit
    return cursor.skip(skip >= 0 and skip or 0)

def mongo_page(cursor, page):
    step = 3
    return mongo_limit(cursor.sort("_id", pymongo.ASCENDING), step * (page + 1)).limit(step)

class StorageEngine:
    client = pymongo.MongoClient('mongodb://falconboard:password@ds161022.mlab.com:61022/falconboard')
    db = client.get_default_database()

    def check_board(self, board):
        if not board in ['b']:
            raise falcon.HTTPError(falcon.HTTP_404, 'Unknown board', 'Board not found')

    def check_post(self, post):
        for field in post:
            if not field in ['title', 'text', 'parent', 'sage']:
                raise falcon.HTTPError(falcon.HTTP_403, 'Invalid argument', 'Unknown field: ' + field)

    def get_posts(self, board):
        self.check_board(board)
        coll = self.db['board.' + board]
        posts = []
        threads = mongo_page(coll.find({"parent": None}), 0)
        for thread in threads:
            comments = mongo_limit(coll.find({"parent": thread["_id"]}), 3)
            posts = posts + [thread] + list(comments)
        return posts

    def get_post(self, board, post):
        self.check_board(board)
        coll = self.db['board.' + board]
        post = coll.find_one({"_id": post})
        comments = coll.find({"parent": post})
        return [post] + list(comments)

    def add_post(self, board, parent, post):
        self.check_board(board)
        self.check_post(post)
        counters = self.db['counters']
        _id = counters.find_and_modify(query={"_id": board}, update={"$inc": {"next": 1}})["next"]
        if _id == None:
            raise falcon.HTTPError(falcon.HTTP_500, 'Counters lost', 'Internal error, can\'t find counter for ' + board)
        coll = self.db['board.' + board]
        post["_id"] = _id
        post["parent"] = parent
        return coll.insert_one(post)



class StorageError(Exception):
    @staticmethod
    def handle(ex, req, resp, params):
        description = ('Sorry, couldn\'t write your thread to the '
                       'database. It worked on my box.')

        raise falcon.HTTPError(falcon.HTTP_725,
                               'Database Error',
                               description)


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

    def on_post(self, req, resp, board, post):
        post = int(post)
        try:
            raw_json = req.stream.read()
        except Exception:
            raise falcon.HTTPError(falcon.HTTP_748,
                                   'Read Error',
                                   'Could not read the request body. Must be '
                                   'them ponies again.')

        try:
            comment = json.loads(raw_json, 'utf-8')
            if not "parent" in comment:
                comment["parent"] = post
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect.')

        comment = self.db.add_post(board, post, comment)

        resp.status = falcon.HTTP_201
        resp.location = '/%s/%s/' % (board, post)

class ThreadsResource:
    def __init__(self, db):
        self.db = db

    def on_get(self, req, resp, board):
        marker = req.get_param('marker') or ''
        limit = req.get_param_as_int('limit') or 50

        try:
            result = self.db.get_posts(board)
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

        post = self.db.add_post(board, None, post)

        resp.status = falcon.HTTP_201
        resp.location = '/%s/%s/' % (board, post.inserted_id)

class DefaultSink(object):
    def on_get(self, req, resp):
        resp.body = str(req.path)

def user_loader(username, password):
    if password != username + '123':
        raise falcon.HTTPError(falcon.HTTP_403, 'Auth failed', 'Incorrect password')
    return {'username': username}
auth_backend = falcon_auth.BasicAuthBackend(user_loader)
auth_middleware = falcon_auth.FalconAuthMiddleware(auth_backend, exempt_routes=['/_api/'], exempt_methods=['HEAD', 'GET', 'POST'])

# Configure your WSGI server to load "threads.app" (app is a WSGI callable)
app = falcon.API(middleware=auth_middleware)

db = StorageEngine()

threads = ThreadsResource(db)
post = PostResource(db)
app.add_route('/_api/{board}', threads)
app.add_route('/_api/{board}/', threads)
app.add_route('/_api/{board}/{post}', post)
app.add_route('/_api/{board}/{post}/', post)
app.add_sink(DefaultSink().on_get, prefix='/')
#app.add_route('/{board}/', BoardResource(db))
#app.add_route('/{board}/{thread}', BoardResource(db))

# If a responder ever raised an instance of StorageError, pass control to
# the given handler.
app.add_error_handler(StorageError, StorageError.handle)

# Useful for debugging problems in your API; works with pdb.set_trace()
if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
