import time
from datetime import datetime
import json

import pika
from faker import Faker

from models import User

fake = Faker()


def seed_fake_users():
    for user in range(10):
        user = User(
            fullname=fake.name(),
            user_email=fake.email(),
            email_sent=False
        )
        user.save()


def get_objid():
    idx = []
    ids = User.objects()
    for u in ids:
        idx.append(str(u.id))
    return idx


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))

    channel = connection.channel()

    channel.exchange_declare(exchange='inform', exchange_type='topic')
    i = 0
    for u in get_objid():
        i += 1
        channel.queue_declare(queue=u, durable=False)
        channel.queue_bind(exchange='inform', queue=u, routing_key='email')

        message = {
            'id': u,
            'payload': f'Task #{i}',
            'date': datetime.now().isoformat()
        }

        channel.basic_publish(
            exchange='inform',
            routing_key='email',
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.TRANSIENT_DELIVERY_MODE
            )
        )
        print(" [x] Sent %r" % message)

        time.sleep(1)

    connection.close()


if __name__ == '__main__':
    seed_fake_users()
    main()

# def main():
#     credentials = pika.PlainCredentials('guest', 'guest')
#     connection = pika.BlockingConnection(
#         pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
#     channel = connection.channel()
#
#     channel.queue_declare(queue='hello_world')
#
#     channel.basic_publish(exchange='', routing_key='hello_world', body='Hello world!'.encode())
#     print(" [x] Sent 'Hello World!'")
#     connection.close()
#
#
# if __name__ == '__main__':
#     main()
