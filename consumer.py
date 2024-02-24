import json

import pika

import sys
import os

from bson import ObjectId

from models import User


def get_objid() -> list[str]:
    idx = []
    ids = User.objects()
    for u in ids:
        idx.append(str(u.id))
    return idx


def callback(ch, method, properties, body):
    print(f" [x] Received {body}")
    pk = json.loads(body.decode('utf-8'))

    user = User.objects(id=ObjectId(pk['id']), email_sent=False).first()
    if user:
        user.update(set__email_sent=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))

    channel = connection.channel()

    for u in get_objid():
        channel.queue_declare(queue=u, durable=False)
        channel.basic_consume(queue=u, on_message_callback=callback)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(os.EX_OSERR)
