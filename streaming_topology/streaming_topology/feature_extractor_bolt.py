from collections import defaultdict
from collections import namedtuple
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('counter')

FEATURE = namedtuple("userId", "type", "timestamp")


class FeatureExtractorBolt(SimpleBolt):

    OUTPUT_FIELDS = FEATURE

    def initialize(self):
        self.words = defaultdict(int)

    def process_tuple(self, tup):
        _user_id, _type, _behavior = tup.values
        timestamp = _behavior["timestamp"]
        # print _user_id, _type, _behavior
        log.debug("{0} {1}".format(_user_id, _type, timestamp))
        # word, = tup.values
        # self.words[word] += 1
        # log.debug("{0} {1}".format(word, self.words[word]))
        # self.emit((word, self.words[word]), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/feature_extractor_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    FeatureExtractorBolt().run()
