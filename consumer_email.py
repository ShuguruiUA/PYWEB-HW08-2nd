import pika

import sys
import os
import json

from models import User


def callback(ch, method, properties, body):
    """
    отримуємо повідомлення у бінароному вигляді JSON, декодуємо його для отримання ObjectID користувача у базі даних
    оновлюємо користувачу інформацію про те що повідомлення було надіслано та повідомляємо що задачу в черзі виконано
    """
    print(f" [x] Received {body}")
    ids = json.loads(body.decode('utf-8'))
    user = User.objects(id=ids['payload'], message_sent=False).first()
    if user:
        user.update(set__message_sent=True)
        print(f' [x] {user.fullname} with ID {user.id} was successfully updated')
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))

    channel = connection.channel()

    channel.queue_declare(queue='email_queue', durable=False)
    channel.basic_consume(queue='email_queue', on_message_callback=callback)

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
