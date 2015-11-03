import logging
import json
from pyleus.storm import SimpleBolt

log = logging.getLogger("regularization")

class RegularizationBolt(SimpleBolt):

    OUTPUT_FIELDS = ["userId", "type", "behavior"]

    def process_tuple(self, tup):
        _raw_data_dict = tup.values[0]

        log.debug("_raw_data_dict")
        log.debug(_raw_data_dict)
        #_key   = _raw_data_dict.keys()[0]
        _value = json.loads(_raw_data_dict.values()[0])
        log.debug(_value.keys())
        log.debug("fuck regular")
        log.debug(_value["timestamp"])
        _user_id  = _value.pop("user_id")
        _type     = _value.pop("type")
        _behavior = _value
        _behavior["timestamp"] = _behavior["timestamp"] / 1000
        # TODO: do some check.

        self.emit((_user_id, _type, _behavior))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/regularization_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    RegularizationBolt().run()
