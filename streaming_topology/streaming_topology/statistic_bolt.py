from collections import defaultdict
from collections import namedtuple
from array import array
import arrow
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("feature_extractor")

# FEATURE = namedtuple("userId", "type", "timestamp")


class StatisticBolt(SimpleBolt):

    # OUTPUT_FIELDS = FEATURE
    TAG = '''|---<Statistic Bolt>---|\n'''

    def initialize(self):
        # The default window will be a empty array. Every element in array will be a integrated tuple of info.
        default_window = lambda: defaultdict(lambda: [])
        # The default feature statistic.
        default_statistic = lambda: {
            "location": {
                "start_time": float("inf"),
                "end_time":   0.0,
                "possible_location": {"lv1": defaultdict(lambda: 0), "lv2": defaultdict(lambda: 0)},
                "gps_trace": []
            },
            "motion": {
                "start_time": float("inf"),
                "end_time":   0.0,
                "possible_motion": defaultdict(lambda: 0)
            }
        }
        # window is used for logging data and outputting to local file.
        self.feature_window = defaultdict(default_window)
        # vector is used for classifying and outputting result.
        self.feature_statistic = defaultdict(default_statistic)
        #
        self.last_timestamp = arrow.utcnow().timestamp

    def process_tick(self):
        # Output log to file.
        log_bolt_tick = "[%s] tick tuple was received." % arrow.utcnow()
        log.debug(log_bolt_tick)
        log_bolt_window = ""
        for user_id, value in self.feature_window.iteritems():
            log_bolt_window += "Info of User %s \n===================================\n" % user_id
            for type, behaviors in value.iteritems():
                log_bolt_window += "In %s Feature Window\n-----------------------------------\n" % type
                for tuple in behaviors:
                    log_bolt_window += "[%s] User Id: %s, Data Id: %s, Data content: %s\n" % \
                                    (arrow.get(tuple["timestamp"]), user_id, tuple["_id"], tuple)
        log.debug(log_bolt_window)
        log_bolt_statistic = ""
        for user_id, value in self.feature_statistic.iteritems():
            log_bolt_statistic += "Statistic of User %s \n===================================\n" % user_id
            for type, statistic in value.iteritems():
                log_bolt_statistic += "In %s Feature Window\n-----------------------------------\n" % type
                for item, value in statistic.iteritems():
                    log_bolt_statistic += "[%s] %s" % (item, value)
        log.debug(log_bolt_statistic)
        # Emit the feature to the next bolt.
        # for user_id, value in self.feature_window.iteritems():
        #     for type, behavior in value.iteritems():
        #         log_bolt_window = "In %s Feature Window\n=================\n" % type
        #         log_bolt_window += "[%s] User Id: %s, Data Id: %s, Data content: %s\n" % \
        #                            (user_id, arrow.get(behavior["timestamp"]), behavior["_id"], behavior)
        #     log.debug(log_bolt_window)
        # Clear the buffer once processing was over.
        self.last_timestamp = arrow.utcnow().timestamp
        self.feature_window.clear()
        self.feature_statistic.clear()

    def process_tuple(self, tup):
        _user_id, _type, _behavior = tup.values
        # Output log to file.
        # log_bolt_rec = "[%s] Data Id: %s, User Id: %s, Type: %s" % (arrow.utcnow(), _behavior["_id"], _user_id, _type)
        # log.debug(log_bolt_rec)

        # If memory is overflow, it should be delete.
        # It is used for logging info.
        self.feature_window[_user_id][_type].append(_behavior)
        # Calculate feature statistic in the window.
        # - start time of feature window
        if arrow.get(_behavior["timestamp"]).timestamp < self.feature_statistic[_user_id][_type]["start_time"]:
            self.feature_statistic[_user_id][_type]["start_time"] = _behavior["timestamp"]
        # - end time of feature window
        if arrow.get(_behavior["timestamp"]).timestamp > self.feature_statistic[_user_id][_type]["end_time"]:
            self.feature_statistic[_user_id][_type]["end_time"] = _behavior["timestamp"]
        # weight of current location.
        w = arrow.get(_behavior["timestamp"]).timestamp - self.last_timestamp
        if _type == "location":
            # - every possible location lv1's weight.
            for location_lv1, prob_lv1 in _behavior["poiProbLv1"].iteritems():
                self.feature_statistic[_user_id][_type]["possible_location"]["lv1"][location_lv1] += prob_lv1 * w
            # - every possible location lv2's weight.
            for location_lv2, prob_lv2 in _behavior["poiProbLv2"].iteritems():
                self.feature_statistic[_user_id][_type]["possible_location"]["lv2"][location_lv2] += prob_lv2 * w
            # - gps trace
                self.feature_statistic[_user_id][_type]["gps_trace"].append({
                    "timestamp": _behavior["timestamp"],
                    "gps": [_behavior["location"]["latitude"], _behavior["location"]["longitude"]]
                })
        elif _type == "motion":
            # - Calculate every possible motion's weight.
            for motion, prob in _behavior["motionProb"].iteritems():
                self.feature_statistic[_user_id][_type]["possible_motion"][motion] += prob * w

        # self.emit((word, self.words[word]), anchors=[tup])



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/feature_extractor_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    StatisticBolt().run()
