Data generation scripts are in the publish folder

rules.json in the config folder defines the rules

In the src folder, running main.py would aggregate the data and then process the rules for any potential alerts.

All the model classes compose a database class instance which provides the core basic functions to interact with Dynamodb

It assumes that bsm_agg_data and bsm_alert_data tables are already created with appropriate partition and sort keys as defined in the code.

We didn't auto-create the tables as this is not generally done as part of the normal application code. Even if auto-created, that would be done separately in a devops process.

The code:
a) Doesn't do detailed error handling.
b) Assumes that there is no gap in data in the given range. If there is a chance of that, one needs to enhance rule processing by storing previous timestamp, and checking whether each timestamp is one minute more than the last.
c) Has only those underlying functions implemented that are necessary to solve the problem statements.