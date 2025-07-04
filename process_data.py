import csv
import json
from copy import copy
from pathlib import Path
from typing import Any, Callable, Iterable
from uuid import uuid4

WFP_ID = "wfp_id"
OUTPUT_FOLDER = Path("output")


def add_wfp_id(item: dict[str, Any]) -> dict[str, Any]:
    """Adds a wfp_id to an object

    We're adding a wfp_id to use as a unique key
    because sometimes objects returned by the Mobilize
    API do not have IDs. We're using a GUID in a separate
    field to avoid collisions if we fabricate an integer
    and insert it into the id field.

    Args:
        item (dict[str, Any]): a dict representing
        one of the entities in our object model, such
        as attendance, person, etc.

    Returns:
        dict[str, Any]: the parameter with wfp_id added
    """
    return {
        **item,
        WFP_ID: str(uuid4()),
    }


def upsert_in_collection(
    collection: dict[int | str, Any],
    item: dict[str, Any],
) -> dict[str, Any]:
    """Adds an item to a collection if the object's id
    (or wfp_id) does not exist in the collection of
    if the item has a modified date later than the
    modified date of the object already in the collection

    Args:
        collection (dict[int  |  str, Any]): items of a particular class, keyed by
        Mobilize id or wfp_id. This function will modify the parameter by
        maybe adding or replacing an object

        item (dict[str, Any]): a dict representing
        one of the entities in our object model, such
        as attendance, person, etc.

    Returns:
        dict[str, Any]: the item in the collection, not necessarily the
        same object passed in the item parameter
    """

    # FIXME (important) dedupe items with only wfp_id
    item_id = item.get("id") or item[WFP_ID]

    if item_id not in collection:
        collection[item_id] = item
    else:
        item_modified_date = item.get("modified_date")
        items_modified_date = collection[item_id].get("modified_date")

        if (
            item_modified_date
            and not items_modified_date
            or (items_modified_date and item_modified_date > items_modified_date)
        ):
            collection[item_id] = item

    return item


def replace_object_with_wfp_id(
    containing_object: dict[str, Any],
    attribute_to_remove: str,
    wfp_id: str,
) -> dict[str, Any]:
    """Remove a contained object from the item and add the
    contained object's wfp_id in its place.

    Args:
        containing_object (dict[str, Any]): a dict representing
        one of the entities in our object model. In practice
        we'll pass only attendances and events to this function.

        attribute_to_remove (str): the contained object's key in the
        containing object

    Returns:
        dict[str, Any]: the transformed object
    """
    containing_object_copy = copy(containing_object)

    del containing_object_copy[attribute_to_remove]
    containing_object_copy[f"{attribute_to_remove}_wfp_id"] = wfp_id

    return containing_object_copy


def normalize_contained_object(
    containing_object: dict[str, Any],
    collection: dict[int | str, Any],
    item_key: str,
    flattener: Callable[[dict[str, Any], str], dict[str, Any]],
) -> dict[str, Any] | None:
    """Normalize an object, i.e., prepare it to be serialized into a csv file

    Specifically
        * add wfp_id to the object
        * flatten the object (refer to the documentation for the flatten and
        flatten_person functions)
        * upsert the item in the collection (refer to the documentation for the
        upsert_to_collection function)

    Args:
        containing_object (dict[str, Any]): a dict representing
        one of the entities in our object model, such
        as attendance, person, etc.

        collection (dict[int  |  str, Any]): items of a particular class, keyed by
        Mobilize id or wfp_id. This function will modify the parameter by
        maybe adding or replacing an object. This function will modify the parameter
        by maybe adding or replacing an object

        item_key (str): the contained object's key in the item

        flattener (Callable[[dict[str, Any], str], dict[str, Any]]): a function
        to flatten the object (refer to the documentation for the
        upsert_to_collection function)

    Returns:
        dict[str, Any]: the item in the collection, not necessarily the
        same object passed in the item parameter
    """
    if item_key not in containing_object:
        return None

    item_with_wfp_id = add_wfp_id(containing_object.get(item_key, {}))
    flattened_item = flattener(item_with_wfp_id, "")

    item_in_collection = upsert_in_collection(collection, flattened_item)

    return item_in_collection


def normalize(
    containing_object: dict[str, Any],
    organizations: dict[int | str, Any],
    timeslots: dict[int | str, Any] | None = None,
    events: dict[int | str, Any] | None = None,
    people: dict[int | str, Any] | None = None,
    attendance_custom_signup_field_values: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Normalize all the objects contained in a containing object. Refer to
    the normalize_a_contained_object to learn what we mean by normalize

    Args:
        containing_object (dict[str, Any]): A containing object to normalize.
        We'll pass attendances to this function, as well as events. We want to
        normalize events because they contain the sponsor field, representing the
        sponsoring organization.

        organizations (dict[int  |  str, Any]): the organizations collection.
        This function will modify the parameter by maybe adding or replacing an object.

        timeslots (dict[int  |  str, Any] | None, optional): the timeslots collection.
        Will be None when the containing object is an event. Defaults to None.
        If not None, this function will modify the parameter by maybe adding or
        replacing an object

        events (dict[int  |  str, Any] | None, optional): the events collection.
        Will be None when the containing object is an event. Defaults to None.
        If not None, this function will modify the parameter by maybe adding or
        replacing an object

        people (dict[int  |  str, Any] | None, optional): the people collection.
        Will be None when the containing object is an event. Defaults to None.
        If not None, this function will modify the parameter by maybe adding or
        replacing an object

        attendance_custom_signup_field_values (list[dict[str, Any]] | None, optional): the
        attendance_custom_signup_field_values collection. Will be None when the
        containing object is an event. Defaults to None. If not None, this function will
        modify the parameter by maybe adding or replacing an object

    Returns:
        dict[str, Any]: the item in the collection, not necessarily the
        same object passed in the item parameter
    """
    containing_object_copy = copy(containing_object)

    if containing_object_copy.get("sponsor"):
        item_in_collection = normalize_contained_object(
            containing_object_copy, organizations, "sponsor", flatten
        )
        if item_in_collection:
            containing_object_copy = replace_object_with_wfp_id(
                containing_object_copy, "sponsor", item_in_collection[WFP_ID]
            )

    if timeslots is not None and containing_object_copy.get("timeslot"):
        item_in_collection = normalize_contained_object(
            containing_object_copy, timeslots, "timeslot", flatten
        )
        if item_in_collection:
            containing_object_copy = replace_object_with_wfp_id(
                containing_object_copy, "timeslot", item_in_collection[WFP_ID]
            )

    if events is not None and containing_object_copy.get("event"):
        # normalize the sponsor in the event object
        event_with_normalized_sponsor = normalize(
            containing_object_copy["event"],
            organizations=organizations,
        )
        containing_object_copy["event"] = event_with_normalized_sponsor

        normalized_event = normalize_contained_object(
            containing_object_copy, events, "event", flatten
        )

        if normalized_event:
            containing_object_copy = replace_object_with_wfp_id(
                containing_object_copy, "event", normalized_event[WFP_ID]
            )

    if people is not None and containing_object_copy.get("person"):
        item_in_collection = normalize_contained_object(
            containing_object_copy, people, "person", flatten_person
        )
        if item_in_collection:
            containing_object_copy = replace_object_with_wfp_id(
                containing_object_copy, "person", item_in_collection[WFP_ID]
            )

    if attendance_custom_signup_field_values is not None:
        custom_signup_field_values = (
            containing_object_copy.get("custom_signup_field_values") or []
        )

        for custom_signup_field_value in custom_signup_field_values:
            attendance_custom_signup_field_values.append(
                {
                    f"attendance_{WFP_ID}": containing_object_copy[WFP_ID],
                    **custom_signup_field_value,
                }
            )

        # using pop in case "custom_signup_field_values" is not in the object
        containing_object_copy.pop("custom_signup_field_values", None)

    return containing_object_copy


def flatten_person(
    person: dict[str, Any],
    key_prefix: str,
) -> dict[str, Any]:
    """In a person object, replace an object representing a phone number,
    email_address or postal address with a single field. Refer to Mobilize's
    documentation about the behavior of these objects. Passes the person parameter
    to the flatten function.

    Args:
        person (dict[str, Any]): an object represeting a person.

        key_prefix (str): passthrough to the call to flatten

    Returns:
        dict[str, Any]: the flattened person
    """
    person_copy = copy(person)

    phone_numbers = person_copy.get("phone_numbers") or []
    email_addresses = person_copy.get("email_addresses") or []
    postal_addresses = person_copy.get("postal_addresses") or []

    for phone_number in phone_numbers:
        person_copy["phone_number"] = phone_number.get("number")
        break

    for email_address in email_addresses:
        person_copy["email_address"] = email_address.get("address")
        break

    for postal_address in postal_addresses:
        person_copy["postal_code"] = postal_address.get("postal_code")
        break

    # using pop in case one of the keys is not in copy_of_person
    person_copy.pop("phone_numbers", None)
    person_copy.pop("email_addresses", None)
    person_copy.pop("postal_addresses", None)

    return flatten(person_copy, key_prefix)


def flatten(
    containing_object: dict[str, Any],
    key_prefix: str,
) -> dict[str, Any]:
    """Objects returned by the Mobilize API contain embedded objects that
    we won't normalize, such as location in the events object. This function takes each
    field in an embedded object and adds it to the containing object, prefixed with the
    string passed in the key_prefix parameter.

    For example,

    {
        "title": "a night with elvis",
        "location": {
            "venue": "hollywood bowl"
        }
    }

    becomes

    {
        "title": "a night with elvis",
        "location_venue": "hollywood bowl"
    }

    Args:
        containing_object (dict[str, Any]): a dict representing
        one of the entities in our object model, such
        as attendance, person, etc.

        key_prefix (str): The prefix to use when adding embedded fields
        to the containing object

    Returns:
        dict[str, Any]: an transformed version of the object passed in the
        containing_object parameter
    """
    to_return: dict[str, Any] = {}

    # Walk through all the key/value pairs in the containing object
    for key, value in containing_object.items():
        key_to_add = key if not key_prefix else f"{key_prefix}_{key}"
        if isinstance(value, dict):
            # if it's a dictionary, recurse into this function to flatten it
            flattened = flatten(containing_object[key], key_to_add)

            # add the flattened object to the containing object
            to_return = to_return | flattened
        elif isinstance(value, list):
            # if it's a list, contcatenate all the truthy members of the list
            # into a single string
            # FIXME (minor) this will filter out zero but the current data has
            # no lists of integers so it's a moot point for now
            non_blank: list[Any] = list(filter(None, value))
            to_return[key_to_add] = ", ".join(non_blank) if non_blank else None
        else:
            to_return[key_to_add] = value

    return to_return


def get_field_names(
    collection: Iterable[dict[str, Any]],
) -> set[str]:
    """Return a set of every key in every object in the collection. We need this
    to create the columns when we write to a CSV

    Args:
        collection (Iterable[dict[str, Any]]): items of a particular class, keyed by
        Mobilize id or wfp_id.

    Returns:
        set[str]: a set of every key in every object in the collection; since it's a set
        the keys in the set are unique
    """
    to_return: set[str] = set()
    for item in collection:
        to_return = to_return | set(item.keys())

    return to_return


def save_one_collection_to_csv(
    collection: Iterable[dict[str, Any]],
    file_name: str,
):
    """Save a single collection to the file specified by file_name

    Args:
        collection (Iterable[dict[str, Any]]): items of a particular class, keyed by
        Mobilize id or wfp_id

        file_name (str): the name of the file into which to serialize the objects; the
        csv extension is optional
    """
    field_names = get_field_names(collection)

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    file_name_with_extension = (
        file_name if file_name.endswith(".csv") else f"{file_name}.csv"
    )

    with open(OUTPUT_FOLDER / file_name_with_extension, mode="w") as file:
        dict_writer = csv.DictWriter(file, fieldnames=sorted(field_names))
        dict_writer.writeheader()
        dict_writer.writerows(collection)


def save_to_csv(
    attendances: dict[int | str, Any],
    organizations: dict[int | str, Any],
    timeslots: dict[int | str, Any],
    events: dict[int | str, Any],
    people: dict[int | str, Any],
    attendance_custom_signup_field_values: Iterable[dict[str, Any]],
):
    """Save each collection to its own CSV. Each argument is one of
    the collections of normalized objects

    Args:
        attendances (dict[int  |  str, Any])
        organizations (dict[int  |  str, Any])
        timeslots (dict[int  |  str, Any])
        events (dict[int  |  str, Any])
        people (dict[int  |  str, Any])
        attendance_custom_signup_field_values (Iterable[dict[str, Any]])
    """
    save_one_collection_to_csv(attendances.values(), "attendances")
    save_one_collection_to_csv(timeslots.values(), "timeslots")
    save_one_collection_to_csv(events.values(), "events")
    save_one_collection_to_csv(people.values(), "people")
    save_one_collection_to_csv(organizations.values(), "organizations")
    save_one_collection_to_csv(
        attendance_custom_signup_field_values, "custom_signup_fields"
    )


def main():
    # Open attendances JSON file
    with open("data/attendances.json") as f:
        attendances_from_file = json.loads(f.read())

    # An attendance represent's a person's participation in something.
    # Attendances contain objects representing a timeslot, an event,
    # a person, and an organization

    # Dictionaries to collect the objects and prepare them to store in CSVs
    # Using dictionaries so we can dedupe based on an ID assigned by Mobilize
    attendances = {}
    timeslots = {}
    events = {}
    people = {}
    organizations = {}

    # This one is a list; these are custom. We want to store each one with no
    # deduping
    attendance_custom_signup_field_values = []

    # Normalize the attendances we read from the file.
    # Extract all the contained objects, add them
    # to their respective collections, and remove them in the containing
    # object, replacing them with their ID
    for attendance in attendances_from_file:
        attendance_with_wfp_id = add_wfp_id(attendance)
        normalized_attendance = normalize(
            containing_object=attendance_with_wfp_id,
            timeslots=timeslots,
            organizations=organizations,
            events=events,
            people=people,
            attendance_custom_signup_field_values=attendance_custom_signup_field_values,
        )

        upsert_in_collection(
            attendances,
            normalized_attendance,
        )

    save_to_csv(
        attendances=attendances,
        organizations=organizations,
        timeslots=timeslots,
        events=events,
        people=people,
        attendance_custom_signup_field_values=attendance_custom_signup_field_values,
    )

    print("processed", len(attendances), "attendances")


if __name__ == "__main__":
    main()
