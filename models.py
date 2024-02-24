from bson import json_util

from mongoengine import connect, Document, StringField, ListField, ReferenceField, CASCADE, EmailField, BooleanField

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

mongo_user = config.get('DB', 'user')
mongodb_pass = config.get('DB', 'pass')
db_name = config.get('DB', 'db_name')
domain = config.get('DB', 'domain')

connect(host=f"""mongodb+srv://{mongo_user}:{mongodb_pass}@{domain}/{db_name}?retryWrites=true&w=majority""", ssl=True)


class User(Document):
    fullname = StringField(max_length=150, required=True)
    user_email = EmailField(unique=True, required=True)
    email_sent = BooleanField()
    meta = {"collection": "users"}

    def to_json(self, *args, **kwargs):
        data = self.to_mongo(*args, **kwargs)
        data["user"] = self.fullname
        return json_util.dumps(data, ensure_ascii=False)