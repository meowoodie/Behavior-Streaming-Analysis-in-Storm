from collections import defaultdict
import arrow
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("statistic")

class EventBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        pass


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/event_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    EventBolt().run()
