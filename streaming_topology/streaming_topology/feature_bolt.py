from collections import defaultdict
from collections import namedtuple
from array import array
from pyleus.storm import SimpleBolt
import arrow
import logging

import math

log = logging.getLogger("feature")


def calculate_speed_trace(gps_trace):
    speed_trace = []
    last_gps = [-1.0, -1.0]
    last_timestamp = 0
    for gps_poi in gps_trace:
        if last_gps != [-1.0, -1.0] or last_timestamp == 0:
            cur_gps       = gps_poi["gps"]
            cur_timestamp = gps_poi["timestamp"]
            if cur_timestamp > last_timestamp:
                distence = math.hypot(last_gps[0] - cur_gps[0], last_gps[1] - cur_gps[1])
                interval = cur_timestamp - last_timestamp
                speed    = distence / interval
                speed_trace.append(speed)
            else:
                log_bolt_error = "Error [calculate_speed_trace] current timestamp %s was smaller than last timestamp %s" % \
                                 (cur_timestamp, last_timestamp)
                log.error(log_bolt_error)
        last_gps = gps_poi["gps"]
        last_timestamp = gps_poi["timestamp"]
    return speed_trace

def normalize_possibility_dict(possibility_dict):
    factor = 1.0 / sum(possibility_dict.itervalues())
    for key in possibility_dict:
        possibility_dict[key] *= factor
    return possibility_dict


class FeatureBolt(SimpleBolt):

    def initialize(self):
        default_vector = lambda: [
            # Length of duration
            arrow.utcnow().timestamp,
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
        length_time  = end_time - start_time
        m_motion     = max(_statistics["motion"]["possible_motion"])
        m_m_prob     = max(normalize_possibility_dict(_statistics["motion"]["possible_motion"]).itervalues())
        m_loc_lv1    = max(_statistics["location"]["possible_location"]["lv1"])
        m_l_lv1_prob = max(normalize_possibility_dict(_statistics["location"]["possible_location"]["lv1"]).itervalues())
        m_loc_lv2 = max(_statistics["location"]["possible_location"]["lv2"])
        m_l_lv2_prob = max(normalize_possibility_dict(_statistics["location"]["possible_location"]["lv2"]).itervalues())
        # - prerequisite
        speed_trace  = calculate_speed_trace(
            sorted(_statistics["location"]["gps_trace"], key=lambda gps_poi: -1 * gps_poi["timestamp"])
        )
        # About speed in feature
        max_speed    = max(speed_trace)
        min_speed    = min(speed_trace)
        ave_speed    = sum(speed_trace) / float(len(speed_trace))

        self.feature_vector[_user_id] = (length_time, start_time, end_time,
                                         m_motion, m_m_prob, m_loc_lv1, m_l_lv1_prob, m_loc_lv2, m_l_lv2_prob,
                                         max_speed, min_speed, ave_speed)

        # Output log to file.
        log_bolt_feature = "\n[%s] Features for user %s: \n" \
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
                           "- Average speed:\t%s\n"
        log.debug(log_bolt_feature)

        self.emit(self.feature_vector[_user_id])

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
