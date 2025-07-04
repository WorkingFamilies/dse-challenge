# Larry Person Technical Challenge Response

## Disclaimers

### Use of AI

None.

### Prior knowledge of the Mobilize API

The DNC syncs data from Mobilize and since March that sync has
been in my team's portfolio. It does what it's supposed to do
and I've never needed to change or even look at the code.

## **Part A: Improve pseudocode script**

I initially approached this as "just refactor the python" and as I did that, I realized ...

* We don't really need to store the data in files since we're immediately storing it in a database
* The interaction with the Mobilize API didn't take into account
  * paging (Mobilize likely won't return every attendance in one call)
  * error responses
* For efficiency, we probably want a highwater-mark sync. The `modified_date` field makes that possible
* Since objects can be modified, maybe we don't want to just load them into the database
  * This reflects a decision I made to build a snapshot of attendances rather than a landing table; this pulls the task of maintaining the snapshot into our code rather than requiring downstream analysts to consider only the most recent update for an attendance

...and I changed the code to address those observations.

## **Part B: Build an ETL pipeline**

### Installation

#### Prerequisites

* `pyenv`
* `pipenv`
* `make`

#### Steps

1. Clone this repository
2. At the root of the repository, run

```sh
make install
```

### Run the script

```sh
make run
```

### Tests

I added some tests in `test_process_data.py`. To run them

```sh
make test
```

### Sample queries

#### How many people RSVPed to an event with a given ID?

Defining RSVP as any response (`REGISTERED`, `CANCELLED`, `CONFIRMED`)

```sql
SELECT COUNT(*) as n
FROM attendances
WHERE
    wfp_event_id = '86f921d9-629d-44bc-9e62-f9e9e441f746'
    AND status IS NOT NULL
```

#### Which event had the most number of completed attendances?

```sql
SELECT *
FROM events
WHERE wfp_id in (
    SELECT event_wfp_id
    FROM attendances
    GROUP BY event_wfp_id
    ORDER BY COUNT(*) desc
    LIMIT 1
)
```

### Approach

* Read the data from `attendances.json`
* Run through the data and extract contained objects
* Store the contained objects in homogeneous dictionaries keyed by an ID
  * Use `modified_date` as a tiebreaker, storing the most recent version of each object instance
* Save the collections to CSV files, one per table

### Decisions and trade offs

#### Data cleaning

I passed through whatever I found in the data in `attendances.json`, treating field values opaquely. This might be an opportunity for future improvement.

#### Tables

In addition to the tables listed in the prompt, I created

* `organizations`
  * Every `attendance` and every `event` embedded in the `attendance` had an embedded `sponsor` (an organization). Rather than repeating the `organization`'s fields for every `attendance` and `event` I decided to normalize it
* `attendance_custom_signup_field_values`
  * This is a one-to-0..n relationship and I went old-school rather than using a column of type `ARRAY`

#### Flattening embedded objects

Rather than using a struct type for embedded objects (such as `location` in the `events` table), I flattened embedded objects. My rationale for this is avoiding implementation differences of composite types across different database systems. For example,

```json
{
    "title": "a night with elvis",
    "location": {
        "venue": "hollywood bowl"
    }
}
```

becomes

```json
{
    "title": "a night with elvis",
    "location_venue": "hollywood bowl"
}
```

#### Handling missing IDs

The `id` field was null in instances of every object type, except
person. In order not to drop important organiziing data on the ground, I added a `wfp_id` to each object to use as a primary key, without which normalization would be impossible. I didn't want to put fake values into the Mobilize `id` fields because that would risk collisions if someone loaded these CSVs into an existing table. I didn't make `wfp_id` an integer because that would require getting the maximum existing `wfp_id` from database tables (which don't exist in this exercise).

There's an opportunity for future improvement here. I did not attempt to dedupe the instances of objects with missing IDs. Naievely that seems like a simple and correct thing to do, but I'd be slightly concerned doing so could result in people mistakenly being recorded as going to the same event. It could also skew statistics about the most-attended events. If I were doing this in real life I would consider reaching out to Mobilize for more context.

#### Schema

I assumed the schema doesn't change because vendors don't change the schema of the API payloads very often and when they do the changes are hopefully backward compatible. I assumed the column names and types in the `CSV` files I create align with the column names and types in the database.
