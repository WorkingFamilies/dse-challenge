import os
from dataclasses import dataclass
from functools import partial
from http import HTTPStatus
from time import sleep
from typing import Any, Callable

import requests
from google.cloud import bigquery

MOBILIZE_BASE_URL = "https://api.mobilize.us/v1/"
ENDPOINT = "attendances"
PER_PAGE = 50

HTTP_RETRY_SLEEP_SECONDS = 60
HTTP_TRIES = 2
RETRYABLE_HTTP_STATUSES = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.NOT_IMPLEMENTED,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]

ORGANIZATION_ID = "1234567"
ATTENDANCES_TABLE = "wfp-data-project.mobilize.attendances"


@dataclass
class PageReturn:
    """Encapsulate the return of a call to the Mobilize API"""

    next_url: str | None = None
    data: list[dict[str, Any]] | None = None


@dataclass
class Field:
    """Information about a field in the payload
    returned from the Mobilize API


    type: a bigquery type

    getter: a function for getting the value from the
    payload
    """

    name: str
    type: str
    getter: Callable[[dict[str, Any]], Any]


def get_person_field(
    key_in_person: str,
    key_in_object: str,
    datum: dict[str, Any] | None,
) -> str | None:
    """Get postal_address, email_address of phone_number
    from the embedded person object. Refer to the
    Mobilize API documentation for information about the
    format of these fields.

    Args:
        key_in_person (str): the key for this object in the
        person object; will be "postal_addresses",
        "email_addresses" or "phone_numbers"

        key_in_object (str): the field we want within
        the object embedded in the person object

        datum (dict[str, Any] | None): one object in the
        payload returned from the Mobilize API

    Returns:
        str | None: The value of the field, if there is one,
        or None
    """
    if datum is None:
        return None

    items = datum.get("person", {}).get(key_in_person)
    return items[0].get(key_in_object) if items else None


SCHEMA = [
    Field(name="id", type="INT64", getter=lambda x: x.get("id")),
    Field(name="created_date", type="INT64", getter=lambda x: x.get("created_date")),
    Field(name="modified_date", type="INT64", getter=lambda x: x.get("modified_date")),
    Field(name="event_id", type="INT64", getter=lambda x: x.get("event", {}).get("id")),
    Field(
        name="event_title",
        type="STRING",
        getter=lambda x: x.get("event", {}).get("title"),
    ),
    Field(
        name="event_type",
        type="STRING",
        getter=lambda x: x.get("event", {}).get("type"),
    ),
    Field(
        name="event_summary",
        type="STRING",
        getter=lambda x: x.get("event", {}).get("summary"),
    ),
    Field(
        name="event_description",
        type="STRING",
        getter=lambda x: x.get("event", {}).get("description"),
    ),
    Field(
        name="person_id", type="INT64", getter=lambda x: x.get("person", {}).get("id")
    ),
    Field(
        name="person_given_name",
        type="STRING",
        getter=lambda x: x.get("person", {}).get("given_name"),
    ),
    Field(
        name="person_family_name",
        type="STRING",
        getter=lambda x: x.get("person", {}).get("family_name"),
    ),
    Field(
        name="person_email_address",
        type="STRING",
        getter=partial(get_person_field, "email_addresses", "address"),
    ),
    Field(
        name="person_phone_number",
        type="STRING",
        getter=partial(get_person_field, "phone_numbers", "number"),
    ),
    Field(
        name="person_postal_code",
        type="STRING",
        getter=partial(get_person_field, "postal_addresses", "postal_code"),
    ),
    Field(
        name="person_sms_opt_in_status",
        type="STRING",
        getter=lambda x: x.get("person", {}).get("sms_opt_in_status"),
    ),
    Field(
        name="start_date",
        type="INT64",
        getter=lambda x: x.get("timeslot", {}).get("start_date"),
    ),
    Field(
        name="end_date",
        type="INT64",
        getter=lambda x: x.get("timeslot", {}).get("end_date"),
    ),
    Field(name="status", type="STRING", getter=lambda x: x.get("status")),
    Field(name="attended", type="BOOL", getter=lambda x: x.get("attended")),
]


def get_max_modified_timestamp(bq_client: bigquery.Client) -> int:
    """Get the most-recent modified_date already in
    the database; this enables a highwater-mark sync

    Args:
        bq_client (bigquery.Client): BigQuery client

    Returns:
        int: the most-recent modified date
    """
    sql = (
        f"SELECT MAX(modified_date) FROM {ATTENDANCES_TABLE} AS max_modified_timestamp"
    )

    result = bq_client.query_and_wait(
        sql,
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )

    try:
        # next gets the first row in the results
        return next(result).max_modified_timestamp
    except StopIteration:
        # if there are no rows in any of the tables
        return 0


def get_page(requests_session: requests.Session, url: str) -> PageReturn:
    """Get a page of data from the Mobilize API.

    Handles retries for potentially transient errors and
    throttling.

    Checks the response for an error.

    Args:
        requests_session (requests.Session)
        url (str)

    Raises:
        RuntimeError: when something goes wrong

    Returns:
        PageReturn: Results of the API request
    """
    for try_number in range(HTTP_TRIES):
        response = requests_session.get(url)
        if response.status_code not in RETRYABLE_HTTP_STATUSES:
            try:
                parsed_response_body = response.json()
                if parsed_response_body.get("error"):
                    raise RuntimeError(
                        f"Error returned from URL {url};"
                        f"{parsed_response_body['error']}"
                    )

                return PageReturn(
                    next_url=parsed_response_body.get("next"),
                    data=parsed_response_body.get("data"),
                )

            except Exception as exception:
                raise RuntimeError(
                    f"Couldn't parse the body returned from URL {url} "
                    f"with HTTP status {response.status_code}; {exception}"
                )
        elif try_number < HTTP_TRIES - 1:
            print(
                f"Got HTTP status {response.status_code} from URL {url},"
                f"will retry in {HTTP_RETRY_SLEEP_SECONDS} seconds"
            )
            sleep(HTTP_RETRY_SLEEP_SECONDS)
            continue
        else:
            raise RuntimeError(
                f"Unexpected HTTP status {response.status_code} from URL"
                f"{url}; no more retries"
            )

    return PageReturn()


def make_save_page_sql(data: list[dict[str, Any]]) -> str:
    """Generate the SQL to persist each page of results
    in BigQuery.

    The strategy for saving each page of results is

    * load the attendances into a temporary table using a parametrized query
    * merge the temporary table into the attendances table

    Args:
        data (list[dict[str, Any]]): the data returned from
        the Mobilize API

    Returns:
        str: the SQL
    """

    fields = [f"{column.name} {column.type}" for column in SCHEMA]

    create_temporary_table_sql = f"""
        CREATE TEMPORARY TABLE temp_attendances
        (
            {"\n".join(fields)}
        );
    """

    # this creates placeholders for one set of values
    # 19 placeholders, one for each column in the temporary table
    #
    # Using placeholders with parameters to handle
    # escaping and malicious SQL prevention
    values_sql = f"({'?' * len(SCHEMA)})"

    # This produces the insert statement
    #
    # If there were two attendances in data, the statement would be
    #
    # INSERT temp_attendances VALUES
    # (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    insert_into_temporary_table_sql = f"""
        INSERT temp_attendances VALUES
        {", ".join([values_sql] * len(data))};
    """

    update_sql = ", ".join([f"{column.name} = T.{column.name}" for column in SCHEMA])

    merge_sql = f"""
        MERGE {ATTENDANCES_TABLE} AS A
            USING temp_attendances AS T
            ON A.id = T.id
        WHEN NOT MATCHED
            INSERT ROW
        WHEN MATCHED
            UPDATE SET
                {update_sql}
    """

    return f"""
        {create_temporary_table_sql}
        {insert_into_temporary_table_sql}
        {merge_sql}
    """


def make_save_page_parameters(
    data: list[dict[str, Any]],
) -> list[bigquery.ScalarQueryParameter]:
    """Convert the fields in the Mobilize API response
    payload to parameters for the parametrized query

    Args:
        data (list[dict[str, Any]]): the data returned from
        the Mobilize API

    Returns:
        list[bigquery.ScalarQueryParameter]: the
        parameters
    """
    to_return: list[bigquery.ScalarQueryParameter] = []
    for datum in data:
        to_return.extend(
            [
                bigquery.ScalarQueryParameter(None, column.type, column.getter(datum))
                for column in SCHEMA
            ]
        )
    return to_return


def save_page(
    bq_client: bigquery.Client,
    data: list[dict[str, Any]],
):
    """Save a page of results from the Mobilize API to
    BigQuery

    Args:
        bq_client (bigquery.Client): BigQuery client
        data (list[dict[str, Any]]): the data returned from
        the Mobilize API
    """
    sql = make_save_page_sql(data)
    parameters = make_save_page_parameters(data)

    bq_client.query_and_wait(
        sql,
        job_config=bigquery.QueryJobConfig(
            use_legacy_sql=False,
            query_parameters=parameters,
        ),
    )


def load_data():
    with bigquery.Client() as bq_client:
        # get the max modified_timestamp in order to get only
        # new and modified entries
        max_modified_timestamp = get_max_modified_timestamp(bq_client)

        headers = dict(Authorization=f"Bearer {os.environ.get("MOBILIZE_API_KEY")}")

        url = f"{MOBILIZE_BASE_URL}/{ORGANIZATION_ID}/{ENDPOINT}?updated_since={max_modified_timestamp}&per_page={PER_PAGE}"

        # use a requests session to avoid reconnecting
        # between pages, which is inefficient
        with requests.session() as requests_session:
            requests_session.headers(headers)

            while url:
                # get the attendances in pages
                page_return = get_page(requests_session, url)

                save_page(bq_client, page_return.data)

                # if Mobilize returned `next` in the response,
                # there are more attendances
                # if they did include `next`, it's the url for
                # the next page of results
                url = page_return.next_url


if __name__ == "__main__":
    load_data()
