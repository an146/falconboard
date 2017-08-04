import json
import logging
import uuid
import traceback
import urlparse
from wsgiref import simple_server

import falcon
import pymongo


logging.basicConfig(level=logging.DEBUG)

boards = ['a', 'b', 'int', 'pr', 'r']

def mongo_limit(cursor, limit):
    skip = cursor.count() - limit
    return cursor.skip(skip >= 0 and skip or 0)

def mongo_page(cursor, page):
    step = 15
    return mongo_limit(cursor.sort("score", pymongo.ASCENDING), step * (page + 1)).limit(step)

def update_post_score(post):
    if not "sages" in post:
        post["sages"] = 0
    if not "max_comment_id" in post:
        post["max_comment_id"] = post["_id"]
    post["score"] = post["max_comment_id"] - post["sages"] * 150

class StorageEngine:
    with open('mongo.url', 'r') as f:
	    client = pymongo.MongoClient(f.read().rstrip())
    db = client.get_default_database()

    def check_board(self, board):
        if not board in boards:
            print board
            raise falcon.HTTPError(falcon.HTTP_404, 'Unknown board', 'Board not found: ' + board)

    def check_post(self, post):
        for field in post:
            if not field in ['email', 'image', 'text', 'parent']:
                raise falcon.HTTPError(falcon.HTTP_403, 'Invalid argument', 'Unknown field: ' + field)
        if len(post['email']) >= 100 or len(post['image']) >= 500 or len(post['text']) >= 20000 or ('parent' in post and not isinstance(post['parent'], (int, long))):
            raise falcon.HTTPError(falcon.HTTP_403, 'Invalid argument', 'Post check failed')

    def sanitize_post(self, post):
        if 'image' in post and post['image'] != "":
            image_parsed = urlparse.urlparse(post['image'])
            if image_parsed[1] in ['upload.wikimedia.org', 'wallpapers.wallhaven.cc', 'i.imgur.com', 'imgur.com', 'image.ibb.co', 'vignette3.wikia.nocookie.net', 'images4.alphacoders.com']:
                post['image'] = urlparse.urlunparse(image_parsed)
            else:
                post['image'] = None
                post['image_link'] = urlparse.urlunparse(image_parsed)

    def update_score(self, coll, _id):
        post = coll.find_one({"_id": _id})
        update_post_score(post)
        coll.update({"_id": _id}, {"$set": {"score": post["score"]}})

    def migrate(self):
        print 'Migrate'
        for board in boards:
            coll = self.db['board.' + board]
            for post in coll.find():
                self.update_score(coll, post["_id"])

    def get_posts(self, board):
        self.check_board(board)
        coll = self.db['board.' + board]
        posts = []
        threads = mongo_page(coll.find({"parent": None}), 0)
        ts = list(threads)
        ts.reverse()
        for thread in ts:
            comments = mongo_limit(coll.find({"parent": thread["_id"]}), 3)
            posts = posts + [thread] + list(comments)
        for post in posts:
            self.sanitize_post(post)
        return posts

    def get_catalog(self, board):
        self.check_board(board)
        coll = self.db['board.' + board]
        posts = []
        for post in coll.find({"parent": None}).sort("score", pymongo.ASCENDING):
            self.sanitize_post(post)
            posts.append(post)
        return posts

    def get_thread(self, board, thread):
        self.check_board(board)
        coll = self.db['board.' + board]
        comments = coll.find({"parent": thread})
        thread = coll.find_one({"_id": thread, "parent": None})
        if thread == None:
            return None
        posts = [thread] + list(comments)
        for post in posts:
            self.sanitize_post(post)
        return posts

    def add_post(self, board, parent, post):
        self.check_board(board)
        self.check_post(post)
        counters = self.db['counters']
        _id = counters.find_and_modify(query={"_id": "next"}, update={"$inc": {"value": 1}})
        if _id == None:
            counters.insert_one({"_id": "next", "value": 100})
            _id = 100
        else:
            _id = int(_id["value"])
        coll = self.db['board.' + board]
        post["_id"] = _id
        post["parent"] = parent
        if parent != None:
            if post["email"].lower() != "sage":
                coll.update({"_id": parent}, {"$set": {"max_comment_id": _id}})
            else:
                coll.update({"_id": parent}, {"$inc": {"sages": 1}})
	    self.update_score(coll, parent)
	else:
            post["max_comment_id"] = _id
            update_post_score(post)
		
        return coll.insert_one(post)



class PostResource:
    def __init__(self, db):
        self.db = db

    def on_get(self, req, resp, board, post):
        try:
            result = self.db.get_thread(board, int(post))
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

        if result == None:
            raise falcon.HTTPError(falcon.HTTP_403, 'Unknown post', 'Maybe another board?')

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
        resp.body = "{}"

class BoardResource:
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
        resp.body = "{}"

class CatalogResource:
    def __init__(self, db):
        self.db = db

    def on_get(self, req, resp, board):
        try:
            result = self.db.get_catalog(board)
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

# Configure your WSGI server to load "threads.app" (app is a WSGI callable)
app = falcon.API()

db = StorageEngine()

board = BoardResource(db)
post = PostResource(db)
catalog = CatalogResource(db)
app.add_route('/{board}', board)
app.add_route('/{board}/', board)
app.add_route('/{board}/catalog', catalog)
app.add_route('/{board}/catalog/', catalog)
app.add_route('/{board}/{post}', post)
app.add_route('/{board}/{post}/', post)

# Useful for debugging problems in your API; works with pdb.set_trace()
if __name__ == '__main__':
    db.migrate()
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
