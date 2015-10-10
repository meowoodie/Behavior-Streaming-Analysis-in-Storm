from collections import defaultdict
from collections import namedtuple
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('counter')

# Counter = namedtuple("Counter", "word count")


class FeatureExtractorBolt(SimpleBolt):

    # OUTPUT_FIELDS = Counter

    def initialize(self):
        # self.words = defaultdict(int)
        pass

    def process_tuple(self, tup):
        _user_id, _type, _behavior = tup.values
        print _user_id, _type, _behavior

        # word, = tup.values
        # self.words[word] += 1
        # log.debug("{0} {1}".format(word, self.words[word]))
        # self.emit((word, self.words[word]), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/word_count_count_words.log',
        format="%(message)s",
        filemode='a',
    )

    FeatureExtractorBolt().run()
