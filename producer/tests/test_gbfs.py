from velib_producer.gbfs import (
    USER_AGENT,
    ChangeFilter,
    bike_types,
    info_messages,
    status_messages,
)

STATUS = {"data": {"stations": [
    {"station_id": 213688169, "stationCode": "16107", "num_bikes_available": 11,
     "num_bikes_available_types": [{"mechanical": 3}, {"ebike": 8}],
     "num_docks_available": 24, "is_installed": 1, "is_returning": 1, "is_renting": 1,
     "last_reported": 1795047571},
    {"station_id": 36255, "stationCode": "9020", "num_bikes_available": 0,
     "num_bikes_available_types": [], "num_docks_available": 30, "is_installed": 1,
     "is_returning": 1, "is_renting": 0, "last_reported": 1795047000},
]}}

INFO = {"data": {"stations": [
    {"station_id": 213688169, "stationCode": "16107", "name": "Benjamin Godard - Victor Hugo",
     "lat": 48.865983, "lon": 2.275725, "capacity": 35}]}}


def test_status_message_shape():
    m = status_messages(STATUS, polled_at=1795051082)[0]
    assert m == {"schema": 1, "station_id": "213688169", "station_code": "16107", "bikes": 11,
                 "mechanical": 3, "ebike": 8, "docks": 24, "is_installed": True,
                 "is_renting": True, "is_returning": True, "last_reported": 1795047571,
                 "polled_at": 1795051082}


def test_bike_types_tolerates_missing_or_empty():
    assert bike_types(None) == (0, 0)
    assert bike_types([{"ebike": 2}, {"unknown": 9}]) == (0, 2)


def test_info_message():
    m = info_messages(INFO, 1)[0]
    assert m["name"].startswith("Benjamin") and m["capacity"] == 35


def test_change_filter_only_forwards_new_reports():
    f = ChangeFilter()
    first = status_messages(STATUS, 1)
    assert len(f(first)) == 2
    assert f(first) == []
    STATUS["data"]["stations"][0]["last_reported"] += 60
    assert [m["station_id"] for m in f(status_messages(STATUS, 2))] == ["213688169"]


def test_user_agent_is_a_valid_header():
    # Un en-tête HTTP doit être en ASCII : un accent ici faisait échouer chaque collecte.
    USER_AGENT.encode("ascii")
