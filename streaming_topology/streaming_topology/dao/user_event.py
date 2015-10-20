from . import user_event
import arrow

FEATURE_KEY = ["duration", "start_time", "end_time",
               "most_motion", "motion_prob",
               "most_location_lv1", "location_lv1_prob", "most_location_lv2", "location_lv2_prob",
               "max_speed", "min_speed", "average_speed"]

def insert_new_feature(user_id, feature):
    db_obj = {
        "user_id": user_id,
        "event_prob": {},
        "createdAt": str(arrow.utcnow()),
        "updatedAt": str(arrow.utcnow())
    }
    return str(user_event.insert_one(db_obj).inserted_id)

