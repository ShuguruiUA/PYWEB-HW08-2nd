import time
from datetime import datetime
import json

import pika
from faker import Faker

from models import User

fake = Faker('uk_UA')

EXCHANGE = 'inform'

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))

channel = connection.channel()

# Визначаємо біржу та її тип
channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

# Створюємо 2 черги
channel.queue_declare(queue='email_queue')
channel.queue_declare(queue='sms_queue')

def main():
    i = 0
    # у циклі випадковим образом створюємо користувача за дапомогою Faker
    for data in range(15):
        i += 1
        method = fake.random_element(elements=('email', 'sms'))
        user = User(fullname=fake.name(),
                    user_email=fake.email(),
                    user_phone=fake.phone_number(),
                    pref_method=method)
        user.save()

        # Створюємо повідомлення для відправки споживачу
        payload = {'id': i, 'payload': str(user.id), 'date': datetime.now().isoformat()}

        # У разі якщо користувач (випадково в даному випадку) обрав відправку електронною поштою публікуємо повідомлення
        # у відповідну чергу
        if method == 'email':
            channel.basic_publish(exchange='', routing_key='email_queue',
                                  body=json.dumps(payload).encode('utf-8'))
            print(f'[*] Message for contact {user.fullname}: {user.user_email} has been added to email_queue')
        else:
            # в протилежному впиадку - смс інформування
            channel.basic_publish(exchange='', routing_key='sms_queue',
                                  body=json.dumps(payload).encode('utf-8'))
            print(f'[*] Message for contact {user.fullname}: {user.user_phone} has been added to sms_queue')

        time.sleep(0.5)
    print(f'Operation complete at {datetime.now().isoformat()}')
    connection.close()


if __name__ == '__main__':
    main()
