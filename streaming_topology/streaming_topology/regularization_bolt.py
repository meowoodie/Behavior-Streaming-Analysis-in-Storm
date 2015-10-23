import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger("regularization")

class RegularizationBolt(SimpleBolt):

    OUTPUT_FIELDS = ["userId", "type", "behavior"]

    def process_tuple(self, tup):
        test = tup.values
        log.debug(test)
        self.emit()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/behavior_streaming/bolt/test_bolt.log',
        format="%(message)s",
        filemode='a',
    )

    RegularizationBolt().run()
