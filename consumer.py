from json import JSONDecodeError

import nsq

from app import search_words
from marshmallow import Schema, fields


class MesssageSchema(Schema):
    address = fields.Str()
    id = fields.Str()
    word = fields.Str()


def handler(message):
    schema = MesssageSchema()
    try:
        result = schema.loads(message.body.decode())
        search_words.delay(result['id'], result['address'], result['word'])
        return True
    except JSONDecodeError:
        return False


r = nsq.Reader(message_handler=handler,
               nsqd_tcp_addresses=['nsqd:4150'],
               topic='topic',
               channel='channel', lookupd_poll_interval=15)

if __name__ == '__main__':
    nsq.run()
