__all__ = [
    # Collections in Mongodb
    "user", "log"
]

# Initiation of Local DB
import pymongo
from pymongo.errors import InvalidId
from bson.objectid import ObjectId
# The location of MongoDB
mongodb_host      = "119.254.111.40"
mongodb_port      = 27017
client_connection = pymongo.MongoClient(host=mongodb_host, port=mongodb_port)
client_connection.the_database.authenticate('root', 'Senz2everyone', source='admin')
# database
db               = client_connection.senz
# collection
behavior_feature = db.BehaviorFeature
user_event       = db.UserEvent

