import streaming_topology.streaming_topology.dao.behavior_feature as t

feature_vector = (21.0, 1444974161.0, 1444974182.0,
                      "walking", "0.270560654941",
                      "traffic", 0.603312544485, "traffic_place", 0.348211068918,
                      0.00598290599097, 0.0, 0.00299145299548)
print t.insert_new_feature("55f7d1057420230d7866e425", feature_vector)