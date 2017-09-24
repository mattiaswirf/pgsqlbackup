# PGSQLBackup

This is just a simple PostgreSQL backup script written in Python, only tested on Ubuntu Linux (as my servers are using Ubuntu). Nothing fancy, it backs up all databases daily and creates a zipfile.

## Usage

* Copy the file settings.example.json to settings.json and change settings.
* The call to pg_dump expects the account to have a [.pgpass](https://www.postgresql.org/docs/9.5/static/libpq-pgpass.html) in users (in this case root) home folder.
* Run the script as root (with sudo).

Feel free to suggest improvements.