__author__ = 'zhanghengyang'

from leancloud import Object
import leancloud

#timeline
appid = "pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81"
appkey = "qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia"

leancloud.init(appid, appkey)

UserEvent = Object.extend("UserEvent")

def save_event(params):

    m_UserEvent = UserEvent()
    for key in params.keys():
        m_UserEvent.set(key, params[key])
    m_UserEvent.save()

    return "sucess"


#save_event({"s":"b"})