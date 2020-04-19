import enum
import json
import datetime

import requests

from celery import Celery
from flask import Flask, request, render_template, url_for
from flask_cors import CORS
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from sqlalchemy import Enum
from marshmallow_enum import EnumField


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://postgres:aezakmi@db/postgres'
app.config['SECRET_KEY'] = 'you-will-never-guess'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

CORS(app)
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)

celery = Celery(app.name, broker='redis://e8-redis', backend='redis://e8-redis')


class Results(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(300), unique=False, nullable=True)
    word = db.Column(db.String(50), unique=False, nullable=True)
    words_count = db.Column(db.Integer, unique=False, nullable=True)
    http_status_code = db.Column(db.Integer)


class TaskStatus(enum.Enum):
    NOT_STARTED = 1
    PENDING = 2
    FINISHED = 3


class Tasks(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(300), unique=False, nullable=True)
    word = db.Column(db.String(50), unique=False, nullable=True)
    timestamp = db.Column(db.DateTime())
    task_status = db.Column(Enum(TaskStatus))
    http_status = db.Column(db.Integer)


class TasksSchema(ma.SQLAlchemySchema):
    class Meta:
        model = Tasks

    id = ma.auto_field()
    address = ma.auto_field()
    word = ma.auto_field()
    timestamp = ma.auto_field()
    task_status = EnumField(TaskStatus)
    http_status = ma.auto_field()


class ResultsSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Results


class NSQD:
    def __init__(self, server):
        self.server = "http://{server}/pub".format(server=server)

    def send(self, topic, msg):
        res = requests.post(self.server, params={"topic": topic}, data=msg)
        if res.ok:
            return res


nsqd = NSQD('nsqd:4151')


@celery.task
def make_url(id):
    task = Tasks.query.get(id)
    task.task_status = 'PENDING'
    db.session.commit()
    address = task.address
    word = task.word
    if not (address.startswith('http') or address.startswith('https')):
        address = 'http://' + address
    nsqd.send('topic', json.dumps({"address": address, "word": word, "id": str(id)}))


@celery.task
def search_words(id, address, word):
    word_counter = 0
    try:
        res = requests.get(address, timeout=10)
        status = res.status_code
        if res.ok:
            words = res.text.lower().split()
            word_counter = words.count(word)
    except requests.RequestException:
        status = 400
    result = Results(address=address, word=word, words_count=word_counter,
                     http_status_code=status)
    task = Tasks.query.get(id)
    task.task_status = 'FINISHED'
    task.http_status = status
    db.session.add(result)
    db.session.commit()


class WordCounter(Resource):
    def post(self):
        data = request.get_json()
        if data:
            url = data['url']
            word = data['word']
            task = Tasks(address=url, timestamp=datetime.datetime.utcnow(), word=word,
                         task_status='NOT_STARTED')
            db.session.add(task)
            db.session.commit()
            make_url.delay(task.id)
            task_from_database = Tasks.query.get_or_404(task.id)
            task_schema = TasksSchema()
            res = task_schema.dump(task_from_database)
            return res, 201
        return 400

    def get(self):
        tasks = Tasks.query.all()
        tasks_schema = TasksSchema(many=True)
        resp = tasks_schema.dump(tasks)
        return resp, 200

class ResultsGetter(Resource):
    def get(self):
        results = Results.query.all()
        results_schema = ResultsSchema(many=True)
        resp = results_schema.dump(results)
        return resp, 200


api.add_resource(WordCounter, '/')
api.add_resource(ResultsGetter, '/results')


db.create_all()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
