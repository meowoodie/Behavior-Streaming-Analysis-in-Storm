import logging
import random

from pyleus.storm import Spout
log = logging.getLogger('counter')

USER_BEHAVIORS_TEST = (
    {"_id": "55f7d37a7420230d7866e41b","type": "motion","motionProb": {"riding": 0.10000000000000000555,"walking": 0.84722632391300500831,"running": 0.75474730745042928337,"driving": 0.99396194970081053199,"sitting": 0.99996538633792275697},"timestamp": 1442304337731.0,"createdAt": 1442304890053,"updatedAt": 1442304890053,"userId": "55f7d1057420230d7866e425"},
    {"_id": "55f7d37e7420230d7866e41c","type": "motion","motionProb": {"riding": 0.1,"walking": 0.847226323913005,"running": 0.7547473074504293,"driving": 0.9939619497008105,"sitting": 0.9999653863379228},"timestamp": 1442304367731,"createdAt": 1442304894534,"updatedAt": 1442304894534,"userId": "55f7d1057420230d7866e414"},
    {"_id": "55f7d5277420230d7866e424","poiProbLv2": {"hotel": 0.0013929902859461252684,"bus_route": 0.13762320402783395634,"traffic_place": 0.023399720699555766334,"subway": 0.23397248711687815281,"business_building": 0.25510147556772455602,"residence": 0.34821106891773184744,"special_hospital": 0.00029905338433068730892},"poiProbLv1": {"hotel": 0.0013929902859461252684,"traffic": 0.39499541184426789631,"estate": 0.60331254448545634794,"healthcare": 0.00029905338433068730892},"type": "location","timestamp": 1442304210710.0,"createdAt": 1442305319932,"updatedAt": 1442305319932,"userId": "55f7d1057420230d7866e425"},
    {"_id": "55f7d53a7420230d7866e427","poiProbLv2": {"hotel": 0.0013929902859461252684,"bus_route": 0.13762320402783395634,"traffic_place": 0.023399720699555766334,"subway": 0.23397248711687815281,"business_building": 0.25510147556772455602,"residence": 0.34821106891773184744,"special_hospital": 0.00029905338433068730892},"poiProbLv1": {"hotel": 0.0013929902859461252684,"traffic": 0.39499541184426789631,"estate": 0.60331254448545634794,"healthcare": 0.00029905338433068730892},"type": "location","timestamp": 1442304300710.0,"createdAt": 1442305338836,"updatedAt": 1442305338836,"userId": "55f7d1057420230d7866e414"}
)

class UserBehaviorSpout(Spout):

    OUTPUT_FIELDS = ["userId", "type", "behavior"]

    def next_tuple(self):
        behavior = random.choice(USER_BEHAVIORS_TEST)
        log.debug(behavior)
        self.emit((behavior["userId"], behavior["type"], behavior))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/user_behavior_spout.log',
        format="%(message)s",
        filemode='a',
    )

    UserBehaviorSpout().run()