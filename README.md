# Unit Test in Airflow

This is an example use of unit testing in Aiflow 3
Mainly using pytest for testing.
The test objective is:
1. Make sure DAG is loaded and has no missing dependency and error syntax
2. Make sure logic in branch decorator is correct
3. Make sure logic in custom operator is correct

This DAG uses PostgreSQL and mail service. 
To do the test make sure you install the required dependency that can be found in `requirements.txt`.

For running the DAG in local environment, you might want to install and configure the database and the mail service. I use <a href="https://mailpit.axllent.org/">Mailpit</a> to test the mail service locally.
The sample data can be found <a href="https://neon.com/postgresql/postgresql-getting-started/postgresql-sample-database">here</a>.

This git is a part of a Medium article that can be found <a>here</a>.

## DAG Use Case
We have a DVD rental shop, and we want to see our overall rental transaction, including overdue rentals. We want to receive MTD reports every week without needing to run anything manually. We use Airflow for this and connect it to our local PostgreSQL database.

<i>This DAG is purposely containing errors to be detected in the testing phase.</i>

## Testing Phase
We use `pyest` to test this DAG. Simply run this command outside the tests folder to test the whole DAG.
```
pytest
```
If you want to test specific file you may run this command.
```
pytest the-script-name.py
```

Happy learning ~