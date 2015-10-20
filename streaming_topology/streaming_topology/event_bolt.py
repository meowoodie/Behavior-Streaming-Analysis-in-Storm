from collections import defaultdict
import arrow
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("statistic")

class EventBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        _user_id, _feature_id, _feature = tup.values

        # Output log to file.
        log_bolt_feature = "\n[%s] Features %s for user %s: \n" % (arrow.utcnow(), _feature_id, _user_id) + \
                           "- Total duration:\t%s\n" \
                           "- Start time:\t%s\n" \
                           "- End time:\t%s\n" \
                           "- Most motion:\t%s\n" \
                           "- motion prob:\t%s\n" \
                           "- Most location lv1:\t%s\n" \
                           "- location lv1 prob:\t%s\n" \
                           "- Most location lv2:\t%s\n" \
                           "- location lv2 prob:\t%s\n" \
                           "- Max speed:\t%s\n" \
                           "- Min speed:\t%s\n" \
                           "- Average speed:\t%s\n" % _feature
        log.debug(log_bolt_feature)






if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/event_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    EventBolt().run()
