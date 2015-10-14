from collections import defaultdict
from collections import namedtuple
from array import array
import arrow
import logging
from pyleus.storm import SimpleBolt

log = logging.getLogger("feature")


class FeatureBolt(SimpleBolt):
    def initialize(self):
        default_vector = lambda: [
            # Start time of feature
            arrow.utcnow().timestamp,
            # End time of feature
            arrow.utcnow().timestamp,
            # Most possible motion type
            "",
            # Most possible motion prob
            -1.0,
            # Most possible location Lv1 type
            "",
            # Most possible location Lv1 prob
            -1.0,
            # Most possible location Lv2 type
            "",
            # Most possible location Lv2 prob
            -1.0,
            # Max speed
            float("inf"),
            # Min speed
            -1.0,
            # Average speed
            -1.0
        ]
        self.feature_vector = defaultdict(default_vector)

    def process_tuple(self, tup):
        _user_id, _statistics = tup.values

        # Output log to file.
        log_bolt_rec = "[%s] User Id: %s, Statistic: %s" % (arrow.utcnow(), _user_id, _statistics)
        log.debug(log_bolt_rec)

        # Validation of statistics
        if not self.validateStatistics(_statistics):
            return

        


    def validateStatistics(self, statistics):
        if statistics["location"]["start_time"] == float("inf") or \
                        statistics["motion"]["start_time"] == float("inf") or \
                        statistics["location"]["end_time"] == 0.0 or \
                        statistics["motion"]["end_time"] == 0.0:
            return False
        if statistics["location"]["possible_location"]["lv1"] == False or \
                        statistics["location"]["possible_location"]["lv2"] == False or \
                        statistics["motion"]["possible_motion"] == False:
            return False
        return True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/feature_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    FeatureBolt().run()
