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
        if not self.validate_statistics(_statistics):
            return

        # Calculate feature vector
        start_time   = min([_statistics["location"]["start_time"], _statistics["motion"]["start_time"]])
        end_time     = max([_statistics["location"]["end_time"], _statistics["motion"]["end_time"]])
        m_motion     = max(_statistics["motion"]["possible_motion"])
        m_m_prob     = max(self.normalize_possibility_dict(_statistics["motion"]["possible_motion"]).itervalues())
        m_loc_lv1    = max(_statistics["location"]["possible_location"]["lv1"])
        m_l_lv1_prob = max(self.normalize_possibility_dict(_statistics["location"]["possible_location"]["lv1"]).itervalues())
        m_loc_lv2    = max(_statistics["location"]["possible_location"]["lv2"])
        m_l_lv2_prob = max(self.normalize_possibility_dict(_statistics["location"]["possible_location"]["lv2"]).itervalues())
        # - prerequisite
        speed_trace  = self.calculate_speed_trace(_statistics["location"]["gps_trace"])
        max_speed    = max(speed_trace)
        min_speed    = min(speed_trace)
        ave_speed    = sum(speed_trace) / float(len(speed_trace))

        self.feature_vector[_user_id] = (start_time, end_time,
                                         m_motion, m_m_prob, m_loc_lv1, m_l_lv1_prob, m_loc_lv2, m_l_lv2_prob,
                                         max_speed, min_speed, ave_speed)
        self.emit(self.feature_vector[_user_id])

    def calculate_speed_trace(self, gps_trace):
        return []

    def normalize_possibility_dict(self, possibility_dict):
        return {}

    def validate_statistics(self, statistics):
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
