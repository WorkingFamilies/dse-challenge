# Working Families Data Engineer Technical Challenge 

Complete the following programming exercise to show us some of your
code\! This exercise is based on work that the Working Families Data
Team does every day: importing and cleaning up organizing data,
structuring it in a useful way, and providing instructions for others
to make use of it.

Please **do not** make extensive use of AI to help with this
challenge. While we are generally in favor of using lots of tooling to
support our work, we want to be able to evaluate baseline coding
styles without AI as an intermediary.

The starter code for this challenge is located on GitHub:

https://github.com/WorkingFamilies/data-engineer-code-challenge

## **Part A: Improve pseudocode script**

Working Families works with thousands of volunteers each year to get
out the vote for our endorsed candidates. We use the popular
progressive events tool, Mobilize, to create events—such as phone
banks, text banks, debate watch parties, and more—that volunteers can
RSVP to. Organizers can manage attendees by confirming their
attendance before an event and marking them as "complete" if they
attended the event. The Working Families Data Team (that’s you) needs
a reproducible, *offline* pipeline analysts can run locally.

Review the script **ingest\_mobilize\_pipeline.py**. This is pseudo-code
(not actually working) to ingest data from the Mobilize API. Review
this script and make changes to the script to improve it. This is an
open-ended task and there isn’t any specific change we’re looking for,
and your task is not to get the script to actually run. Edit the
script to be better designed, implemented, documented, etc.

## **Part B: Build an ETL pipeline**

### **Task 1**
Starting with the **process\_data.py** script, parse the sample JSON data
from the Mobilize attendances API to save a series of flat files with
data on attendances, timeslots, events, & people. There is
documentation on the data model [here.](https://github.com/mobilizeamerica/api#attendances) Create the tables in the format
which you would if you were putting this data into a database and save
them as flat files. The tables should be placed in the output folder:
one for each of attendances, timeslots, events, and people. Each file
should contain all of the relevant information and related IDs for the
table. This one isn’t pseudocode \- your code should actually run
successfully here.

### **Task 2: Tests**
Add a bit of test coverage, up to three tests that ensure your code is
working as intended or that the data looks approp\riate.

### **Task 3: Documentation and instructions** 

Once you are done structuring your data, provide a written summary of
your approach and any notes or limitations of your implementation. If
you weren’t able to complete the task, document what you were and
weren’t able to do.

Additionally, provide high level instructions (no code necessary, but
describe tables, fields and foreign keys) for an analyst to query your
database to answer the following questions:

1\. How many people RSVP’d to an event with a given ID?

2\. What event had the most number of completed attendances?

## **Submitting your work** 

Submissions can be submitted as a zip file of your work emailed to
aweisgrau@workingfamilies.org or as a pull request against **your** fork
of the original repository. Please do not submit a pull request to the
original repository.

## **Time limits**

There’s no time limit, but don’t feel the need to go over the top with
your submission. We expect this task to take up to four hours to
complete, but it could take more or less time depending on your
familiarity with python and data.

## **Evaluation criteria** 

For Part 1A, we’ll be evaluating your overall approach to what
well-designed code should look like. For Part 1B, we’ll be evaluating
whether the code works, as well as its quality. Before submitting,
make sure that your code does what you expect it to do, that it’s
clean and neat enough to meet your standards with appropriate inline
comments, and that you’ve provided us with written documentation and
instructions on how to run it. Your tests should pass!
