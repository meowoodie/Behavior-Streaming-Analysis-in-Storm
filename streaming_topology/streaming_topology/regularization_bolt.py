import logging
import json
from pyleus.storm import SimpleBolt

log = logging.getLogger("regularization")

class RegularizationBolt(SimpleBolt):

    OUTPUT_FIELDS = ["userId", "type", "behavior"]

    def process_tuple(self, tup):
        _raw_data_str = tup.values[0]
        _raw_data_dict = json.dumps(_raw_data_str)

        log.debug(_raw_data_str)

        _key   = _raw_data_dict.keys()[0]
        _value = _raw_data_dict.value()[0]

        _user_id  = _value.pop("user_id")
        _type     = _value.pop("type")
        _behavior = _raw_data_dict

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
