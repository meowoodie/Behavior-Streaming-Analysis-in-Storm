from collections import defaultdict
from collections import namedtuple
from array import array
import arrow
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("feature_extractor")

# FEATURE = namedtuple("userId", "type", "timestamp")


class FeatureExtractorBolt(SimpleBolt):

    # OUTPUT_FIELDS = FEATURE

    def initialize(self):
        # The two measures to be joined will be stored togheter in an array,
        # where the inf value means that the measure is missing
        default = lambda: 

        # The array will be stored in a dictionary using as key the sensor_id
        # which produced the measures
        self.feature_window = defaultdict(default)

    def process_tick(self):
        # Output log to file.
        log_bolt_tick = "[%s] tick tuple was received." % arrow.utcnow()
        log.debug(log_bolt_tick)


    def process_tuple(self, tup):
        _user_id, _type, _behavior = tup.values
        # Output log to file.
        log_bolt_rec = "[%s] Data Id: %s, User Id: %s, Type: %s" % (arrow.utcnow(), _behavior["_id"], _user_id, _type)
        log.debug(log_bolt_rec)
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
