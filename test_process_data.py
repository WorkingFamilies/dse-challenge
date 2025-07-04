import json

import process_data
from process_data import WFP_ID, add_wfp_id, flatten, flatten_person, normalize

TEST_ATTENDANCE = """{
    "created_date": 1556031669,
    "timeslot": {
      "is_full": false,
      "start_date": 1556028000,
      "id": 541134,
      "instructions": null,
      "end_date": 1556071200
    },
    "sponsor": {
      "race_type": null,
      "created_date": 1554242510,
      "slug": "workingfamilies",
      "modified_date": 1657227394,
      "name": "Working Families Party",
      "district": "",
      "candidate_name": "",
      "id": 1391,
      "org_type": "PARTY_COMMITTEE",
      "event_feed_url": "https://www.mobilize.us/workingfamilies/",
      "logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
      "is_independent": true,
      "is_coordinated": false,
      "state": "",
      "is_primary_campaign": false
    },
    "modified_date": 1559850718,
    "rating": "Positive",
    "person": {
      "email_addresses": [
        {
          "primary": true,
          "address": "enialis.liadon@example.com"
        }
      ],
      "phone_numbers": [
        {
          "primary": true,
          "number": "5555555555"
        }
      ],
      "sms_opt_in_status": "OPT_IN",
      "created_date": 1554167964,
      "family_name": "Liadon",
      "modified_date": 1664202529,
      "blocked_date": null,
      "person_id": 467354,
      "id": 467354,
      "user_id": 474960,
      "postal_addresses": [
        {
          "primary": true,
          "postal_code": "10031"
        }
      ],
      "given_name": "Enialis"
    },
    "event": {
      "accessibility_notes": null,
      "approval_status": "APPROVED",
      "sponsor": {
        "race_type": null,
        "created_date": 1554242510,
        "slug": "fake_workingfamilies",
        "modified_date": 1657227394,
        "name": "Fake Working Families Party",
        "district": "",
        "candidate_name": "",
        "id": 1392,
        "org_type": "PARTY_COMMITTEE",
        "event_feed_url": "https://www.mobilize.us/workingfamilies/",
        "logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
        "is_independent": true,
        "is_coordinated": false,
        "state": "",
        "is_primary_campaign": false
      },
      "created_by_volunteer_host": false,
      "modified_date": 1601665109,
      "event_campaign": null,
      "location": {
        "venue": "",
        "address_lines": [
          "line_1",
          "line_2"
        ],
        "locality": "",
        "region": "",
        "country": "US",
        "postal_code": "",
        "location": {
          "latitude": 1.0,
          "longitude": 2.0
        },
        "congressional_district": null,
        "state_leg_district": null,
        "state_senate_district": null
      },
      "contact": {
        "name": "Laucian Meliamne",
        "email_address": "laucian.meliamne@example.com",
        "phone_number": "555-555-5555",
        "owner_user_id": 474960
      },
      "id": 91154,
      "instructions": "",
      "timezone": "America/Chicago",
      "virtual_action_url": "https://www.wfp4themany.org/text",
      "featured_image_url": "https://mobilizeamerica.imgix.net/uploads/event/Copy%20of%20TX%20Endorsed%20%281%29_20190423144959602944.png",
      "browser_url": "https://www.mobilize.us/workingfamilies/event/91154/",
      "tags": null,
      "title": "Text Out the Vote Texas!",
      "event_type": "TEXT_BANK",
      "summary": "In 2019, it's Our Turn to show up. It's Our Turn to run. It's Our Turn to decide.",
      "address_visibility": "PUBLIC",
      "high_priority": null,
      "created_date": 1556031010,
      "accessibility_status": null,
      "visibility": "PUBLIC",
      "timeslots": null,
      "is_virtual": true,
      "description": "fake_description"
    },
    "custom_signup_field_values": [
      {
        "text_value": "Yes We Can Working Families",
        "custom_field_name": "Organization",
        "custom_field_id": 51,
        "boolean_value": null
      }
    ],
    "feedback": "wfp is my political home",
    "status": "REGISTERED",
    "id": 961149,
    "attended": true,
    "referrer": {
      "utm_source": null,
      "utm_medium": null,
      "utm_campaign": null,
      "utm_term": null,
      "utm_content": null,
      "url": "https://t.co/M8mGyMnJWL"
    }
  }"""

NEWER_TEST_ATTENDANCE = """{
    "created_date": 1556031669,
    "timeslot": {
      "is_full": false,
      "start_date": 1556028000,
      "id": 541134,
      "instructions": null,
      "end_date": 1556071200
    },
    "sponsor": {
      "race_type": null,
      "created_date": 1554242510,
      "slug": "workingfamilies",
      "modified_date": 1657227394,
      "name": "Working Families Party",
      "district": "",
      "candidate_name": "",
      "id": 1391,
      "org_type": "PARTY_COMMITTEE",
      "event_feed_url": "https://www.mobilize.us/workingfamilies/",
      "logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
      "is_independent": true,
      "is_coordinated": false,
      "state": "",
      "is_primary_campaign": false
    },
    "modified_date": 1559850718,
    "rating": "Positive",
    "person": {
      "email_addresses": [
        {
          "primary": true,
          "address": "enialis.liadon@example.com"
        }
      ],
      "phone_numbers": [
        {
          "primary": true,
          "number": "5555555555"
        }
      ],
      "sms_opt_in_status": "OPT_IN",
      "created_date": 1554167964,
      "family_name": "Liadon",
      "modified_date": 1664202529,
      "blocked_date": null,
      "person_id": 467354,
      "id": 467354,
      "user_id": 474960,
      "postal_addresses": [
        {
          "primary": true,
          "postal_code": "10031"
        }
      ],
      "given_name": "Enialis"
    },
    "event": {
      "accessibility_notes": null,
      "approval_status": "APPROVED",
      "sponsor": {
        "race_type": null,
        "created_date": 1554242510,
        "slug": "fake_workingfamilies",
        "modified_date": 1657227399,
        "name": "Fake Working Families Party",
        "district": "",
        "candidate_name": "",
        "id": 1392,
        "org_type": "PARTY_COMMITTEE",
        "event_feed_url": "https://www.mobilize.us/workingfamilies/",
        "logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
        "is_independent": true,
        "is_coordinated": false,
        "state": "",
        "is_primary_campaign": false
      },
      "created_by_volunteer_host": false,
      "modified_date": 1601665109,
      "event_campaign": null,
      "location": {
        "venue": "",
        "address_lines": [
          "",
          ""
        ],
        "locality": "",
        "region": "",
        "country": "US",
        "postal_code": "",
        "location": {
          "latitude": null,
          "longitude": null
        },
        "congressional_district": null,
        "state_leg_district": null,
        "state_senate_district": null
      },
      "contact": {
        "name": "Laucian Meliamne",
        "email_address": "laucian.meliamne@example.com",
        "phone_number": "555-555-5555",
        "owner_user_id": 474960
      },
      "id": 91154,
      "instructions": "",
      "timezone": "America/Chicago",
      "virtual_action_url": "https://www.wfp4themany.org/text",
      "featured_image_url": "https://mobilizeamerica.imgix.net/uploads/event/Copy%20of%20TX%20Endorsed%20%281%29_20190423144959602944.png",
      "browser_url": "https://www.mobilize.us/workingfamilies/event/91154/",
      "tags": null,
      "title": "Text Out the Vote Texas!",
      "event_type": "TEXT_BANK",
      "summary": "In 2019, it's Our Turn to show up. It's Our Turn to run. It's Our Turn to decide.",
      "address_visibility": "PUBLIC",
      "high_priority": null,
      "created_date": 1556031010,
      "accessibility_status": null,
      "visibility": "PUBLIC",
      "timeslots": null,
      "is_virtual": true,
      "description": "fake_description"
    },
    "custom_signup_field_values": [
      {
        "text_value": "Yes We Can Working Families",
        "custom_field_name": "Organization",
        "custom_field_id": 51,
        "boolean_value": null
      }
    ],
    "feedback": "wfp is my political home",
    "status": "REGISTERED",
    "id": 961149,
    "attended": true,
    "referrer": {
      "utm_source": null,
      "utm_medium": null,
      "utm_campaign": null,
      "utm_term": null,
      "utm_content": null,
      "url": "https://t.co/M8mGyMnJWL"
    }
  }"""

TEST_ATTENDANCE_WITH_NULL_IDS = """{
    "created_date": 1585860020,
    "sponsor": {
      "created_date": 1554242510,
      "race_type": null,
      "district": "",
      "is_primary_campaign": false,
      "event_feed_url": "https://www.mobilize.us/workingfamilies/",
      "state": "",
      "is_independent": true,
      "slug": "workingfamilies",
      "logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
      "id": 1391,
      "name": "Working Families Party",
      "candidate_name": "",
      "org_type": "PARTY_COMMITTEE",
      "modified_date": 1657227394,
      "is_coordinated": false
    },
    "attended": null,
    "referrer": {
      "utm_source": null,
      "utm_medium": null,
      "utm_campaign": null,
      "utm_term": null,
      "utm_content": null,
      "url": null
    },
    "feedback": null,
    "status": null,
    "person": {
      "created_date": 1576831670,
      "phone_numbers": [
        {
          "primary": true,
          "number": "5555555555"
        }
      ],
      "sms_opt_in_status": "UNSPECIFIED",
      "email_addresses": [
        {
          "primary": true,
          "address": "paelias.nailo@example.com"
        }
      ],
      "id": 970647,
      "given_name": "Paelias",
      "postal_addresses": [
        {
          "primary": true,
          "postal_code": "56220"
        }
      ],
      "blocked_date": 1634070923,
      "modified_date": 1634070926,
      "user_id": 995233,
      "family_name": "Nailo",
      "person_id": 970647
    },
    "id": null,
    "custom_signup_field_values": [],
    "rating": null,
    "timeslot": {
      "id": null,
      "instructions": null,
      "end_date": 1585873800,
      "is_full": false,
      "start_date": 1585866600
    },
    "modified_date": 1634071146,
    "event": {
      "created_date": null,
      "sponsor": null,
      "accessibility_status": null,
      "browser_url": null,
      "location": null,
      "is_virtual": true,
      "tags": null,
      "modified_date": null,
      "timeslots": null,
      "approval_status": "APPROVED",
      "timezone": "America/New_York",
      "event_type": "MEETING",
      "created_by_volunteer_host": false,
      "summary": null,
      "contact": null,
      "virtual_action_url": null,
      "visibility": "PUBLIC",
      "title": null,
      "featured_image_url": null,
      "id": null,
      "high_priority": null,
      "instructions": null,
      "accessibility_notes": null,
      "address_visibility": "PUBLIC",
      "event_campaign": null,
      "description": null
    }
  }
"""


def normalize_attendance(mocker):
    timeslots = {}
    events = {}
    people = {}
    organizations = {}
    attendance_custom_signup_field_values = []

    test_attendance = json.loads(TEST_ATTENDANCE)

    flatten_spy = mocker.spy(process_data, "flatten")
    flatten_person_spy = mocker.spy(process_data, "flatten_person")

    test_attendance_with_wfp_id = add_wfp_id(test_attendance)
    normalized_attendance = normalize(
        containing_object=test_attendance_with_wfp_id,
        timeslots=timeslots,
        organizations=organizations,
        events=events,
        people=people,
        attendance_custom_signup_field_values=attendance_custom_signup_field_values,
    )

    # all unique objects added to the collections
    assert len(timeslots) == 1
    assert 541134 in timeslots

    assert len(events) == 1
    assert 91154 in events

    assert len(people) == 1
    assert 467354 in people

    assert len(organizations) == 2
    assert 1391 in organizations
    assert 1392 in organizations

    assert len(attendance_custom_signup_field_values) == 1
    assert attendance_custom_signup_field_values[0] == {
        "attendance_wfp_id": test_attendance_with_wfp_id[WFP_ID],
        "text_value": "Yes We Can Working Families",
        "custom_field_name": "Organization",
        "custom_field_id": 51,
        "boolean_value": None,
    }

    # there's a wfp_id for each contained object
    assert "event_wfp_id" in normalized_attendance
    assert "timeslot_wfp_id" in normalized_attendance
    assert "person_wfp_id" in normalized_attendance
    assert "sponsor_wfp_id" in normalized_attendance
    assert "sponsor_wfp_id" in events[91154]

    # and the contained object is gone
    assert "event" not in normalized_attendance
    assert "timeslot" not in normalized_attendance
    assert "person" not in normalized_attendance
    assert "sponsor" not in normalized_attendance
    assert "sponsor" not in events[91154]

    # flatten got called as expected
    assert len(flatten_spy.call_args_list) == 8

    assert flatten_spy.call_args_list[0].args[0] == organizations[1391]
    assert flatten_spy.call_args_list[1].args[0] == timeslots[541134]
    assert flatten_spy.call_args_list[2].args[0] == organizations[1392]

    expected_event_passed_to_flatten = test_attendance["event"]
    del expected_event_passed_to_flatten["sponsor"]
    expected_event_passed_to_flatten["sponsor_wfp_id"] = organizations[1392]["wfp_id"]
    expected_event_passed_to_flatten["wfp_id"] = events[91154]["wfp_id"]
    assert flatten_spy.call_args_list[3].args[0] == expected_event_passed_to_flatten

    assert flatten_spy.call_args_list[4].args[0] == test_attendance["event"]["location"]
    assert (
        flatten_spy.call_args_list[5].args[0]
        == test_attendance["event"]["location"]["location"]
    )
    assert flatten_spy.call_args_list[6].args[0] == test_attendance["event"]["contact"]
    assert flatten_spy.call_args_list[7].args[0] == people[467354]

    assert len(flatten_person_spy.call_args_list) == 1


def test_items_without_mobilize_ids_are_included(mocker):
    timeslots = {}
    events = {}
    people = {}
    organizations = {}
    attendance_custom_signup_field_values = []

    test_attendance = json.loads(TEST_ATTENDANCE_WITH_NULL_IDS)

    flatten_spy = mocker.spy(process_data, "flatten")
    flatten_person_spy = mocker.spy(process_data, "flatten_person")

    test_attendance_with_wfp_id = add_wfp_id(test_attendance)
    normalized_attendance = normalize(
        containing_object=test_attendance_with_wfp_id,
        timeslots=timeslots,
        organizations=organizations,
        events=events,
        people=people,
        attendance_custom_signup_field_values=attendance_custom_signup_field_values,
    )

    # all unique objects added to the collections
    assert len(timeslots) == 1
    assert len(events) == 1
    assert len(people) == 1
    assert len(organizations) == 1
    assert len(attendance_custom_signup_field_values) == 0

    # there's a wfp_id for each contained object
    assert "event_wfp_id" in normalized_attendance
    assert "timeslot_wfp_id" in normalized_attendance
    assert "person_wfp_id" in normalized_attendance
    assert "sponsor_wfp_id" in normalized_attendance
    assert "sponsor_wfp_id" not in list(events.values())[0]

    # and the contained object is gone
    assert "event" not in normalized_attendance
    assert "timeslot" not in normalized_attendance
    assert "person" not in normalized_attendance
    assert "sponsor" not in normalized_attendance
    assert "sponsor" in list(events.values())[0]

    # flatten got called as expected
    assert len(flatten_spy.call_args_list) == 4

    assert flatten_spy.call_args_list[0].args[0] == list(organizations.values())[0]
    assert flatten_spy.call_args_list[1].args[0] == list(timeslots.values())[0]

    expected_event_passed_to_flatten = test_attendance["event"]
    expected_event_passed_to_flatten["sponsor"] = None
    expected_event_passed_to_flatten["wfp_id"] = list(events.values())[0]["wfp_id"]
    assert flatten_spy.call_args_list[2].args[0] == expected_event_passed_to_flatten

    assert flatten_spy.call_args_list[3].args[0] == list(people.values())[0]

    assert len(flatten_person_spy.call_args_list) == 1


def test_more_recently_modified_item_replaces_existing_item_in_the_collection():
    timeslots = {}
    events = {}
    people = {}
    organizations = {}
    attendance_custom_signup_field_values = []

    test_attendance = json.loads(TEST_ATTENDANCE)
    test_attendance_with_wfp_id = add_wfp_id(test_attendance)
    normalize(
        containing_object=test_attendance_with_wfp_id,
        timeslots=timeslots,
        organizations=organizations,
        events=events,
        people=people,
        attendance_custom_signup_field_values=attendance_custom_signup_field_values,
    )

    newer_test_attendance = json.loads(NEWER_TEST_ATTENDANCE)
    newer_test_attendance_with_wfp_id = add_wfp_id(newer_test_attendance)
    normalize(
        containing_object=newer_test_attendance_with_wfp_id,
        timeslots=timeslots,
        organizations=organizations,
        events=events,
        people=people,
        attendance_custom_signup_field_values=attendance_custom_signup_field_values,
    )

    # all unique objects added to the collections
    assert len(timeslots) == 1
    assert len(events) == 1
    assert len(people) == 1
    assert len(organizations) == 2
    assert len(attendance_custom_signup_field_values) == 2

    assert (
        list(events.values())[0]["modified_date"]
        == newer_test_attendance_with_wfp_id["event"]["modified_date"]
    )


def test_flatten():
    test_attendance = json.loads(TEST_ATTENDANCE)
    flattened = flatten(test_attendance["event"], "")

    assert flattened == {
        "accessibility_notes": None,
        "accessibility_status": None,
        "address_visibility": "PUBLIC",
        "approval_status": "APPROVED",
        "browser_url": "https://www.mobilize.us/workingfamilies/event/91154/",
        "contact_email_address": "laucian.meliamne@example.com",
        "contact_name": "Laucian Meliamne",
        "contact_owner_user_id": 474960,
        "contact_phone_number": "555-555-5555",
        "created_by_volunteer_host": False,
        "created_date": 1556031010,
        "description": "fake_description",
        "event_campaign": None,
        "event_type": "TEXT_BANK",
        "featured_image_url": "https://mobilizeamerica.imgix.net/uploads/event/Copy%20of%20TX%20Endorsed%20%281%29_20190423144959602944.png",
        "high_priority": None,
        "id": 91154,
        "instructions": "",
        "is_virtual": True,
        "location_address_lines": "line_1, line_2",
        "location_congressional_district": None,
        "location_country": "US",
        "location_locality": "",
        "location_location_latitude": 1.0,
        "location_location_longitude": 2.0,
        "location_postal_code": "",
        "location_region": "",
        "location_state_leg_district": None,
        "location_state_senate_district": None,
        "location_venue": "",
        "modified_date": 1601665109,
        "sponsor_candidate_name": "",
        "sponsor_created_date": 1554242510,
        "sponsor_district": "",
        "sponsor_event_feed_url": "https://www.mobilize.us/workingfamilies/",
        "sponsor_id": 1392,
        "sponsor_is_coordinated": False,
        "sponsor_is_independent": True,
        "sponsor_is_primary_campaign": False,
        "sponsor_logo_url": "https://mobilize-uploads-prod.s3.us-east-2.amazonaws.com/uploads/organization/WFP-WORDMARK-HORIZONTAL%5Bworking-orange%5D_20201005234346171299.png",
        "sponsor_modified_date": 1657227394,
        "sponsor_name": "Fake Working Families Party",
        "sponsor_org_type": "PARTY_COMMITTEE",
        "sponsor_race_type": None,
        "sponsor_slug": "fake_workingfamilies",
        "sponsor_state": "",
        "summary": "In 2019, it's Our Turn to show up. It's Our Turn to run. It's Our Turn to "
        "decide.",
        "tags": None,
        "timeslots": None,
        "timezone": "America/Chicago",
        "title": "Text Out the Vote Texas!",
        "virtual_action_url": "https://www.wfp4themany.org/text",
        "visibility": "PUBLIC",
    }


def test_flatten_person():
    test_attendance = json.loads(TEST_ATTENDANCE)
    flattened = flatten_person(test_attendance["person"], "")

    assert flattened == {
        "blocked_date": None,
        "created_date": 1554167964,
        "email_address": "enialis.liadon@example.com",
        "family_name": "Liadon",
        "given_name": "Enialis",
        "id": 467354,
        "modified_date": 1664202529,
        "person_id": 467354,
        "phone_number": "5555555555",
        "postal_code": "10031",
        "sms_opt_in_status": "OPT_IN",
        "user_id": 474960,
    }
