from collections import defaultdict
import arrow
import logging
from dao.leancloud_dao import save_event
from pyleus.storm import SimpleBolt
import random

log = logging.getLogger("event")

class EventBolt(SimpleBolt):

    event_list = [
        {
  "user": {
    "__type": "Pointer",
    "className": "_User",
    "objectId": "563065f660b26267d31fad74"
  },
  "event": {
    "work_in_office": 1
  },
   "timestamp":1,
  "endTime": {
    "__type": "Date",
    "iso": "2015-10-29T15:20:12.000Z"
  },
  "startTime": {
    "__type": "Date",
    "iso": "2015-10-29T14:49:22.000Z"
  }
},
        {
  "user": {
    "__type": "Pointer",
    "className": "_User",
    "objectId": "563065f660b26267d31fad74"
  },
  "timestamp": 1,
  "event": {
    "dining_in_restaurant": 1
  },
  "endTime": {
    "__type": "Date",
    "iso": "2015-10-29T15:20:12.000Z"
  },
  "startTime": {
    "__type": "Date",
    "iso": "2015-10-29T14:49:22.000Z"
  }
},
        {
  "user": {
    "__type": "Pointer",
    "className": "_User",
    "objectId": "563065f660b26267d31fad74"
  },
  "timestamp": 1,
  "event": {
    "shopping_in_mall": 1
  },
  "endTime": {
    "__type": "Date",
    "iso": "2015-10-29T15:20:12.000Z"
  },
  "startTime": {
    "__type": "Date",
    "iso": "2015-10-29T14:49:22.000Z"
  }
}
    ]

    def initialize(self):
        pass

    def process_tuple(self, tup):
        _user_id, _feature_id, _feature = tup.values

        # Output log to file.
        # log_bolt_feature = "\n[%s] Features %s for user %s: \n" % (arrow.utcnow(), _feature_id, _user_id) + \
        #                    "- Total duration:\t%s\n" \
        #                    "- Start time:\t%s\n" \
        #                    "- End time:\t%s\n" \
        #                    "- Most motion:\t%s\n" \
        #                    "- motion prob:\t%s\n" \
        #                    "- Most location lv1:\t%s\n" \
        #                    "- location lv1 prob:\t%s\n" \
        #                    "- Most location lv2:\t%s\n" \
        #                    "- location lv2 prob:\t%s\n" \
        #                    "- Max speed:\t%s\n" \
        #                    "- Min speed:\t%s\n" \
        #                    "- Average speed:\t%s\n" % _feature
        # log.debug(log_bolt_feature)

        temp = random.choice(self.event_list)
        temp["user"]["objectId"] = _user_id
        temp["timestamp"] = arrow.utcnow().timestamp
        temp["startTime"],temp["endTime"] = _feature[1], _feature[2]
        log.debug("hi")
        log.debug(str(arrow.utcnow()))
        log.debug(save_event(temp))






if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/event_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    EventBolt().run()
