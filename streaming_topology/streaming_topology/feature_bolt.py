from collections import defaultdict
from collections import namedtuple
from array import array
import arrow
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("feature")

class FeatureBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        _user_id, _statistics = tup.values



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/feature_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    FeatureBolt().run()
