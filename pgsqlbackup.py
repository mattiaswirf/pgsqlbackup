#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Imports
import os
import datetime
from shlex import split as xsplit
import subprocess
from subprocess import check_output, check_call, CalledProcessError
import json
import zipfile
import logging
import psycopg2

# psql -h localhost postgres postgres
# SELECT d.datname as "Name" FROM pg_catalog.pg_database d ORDER BY 1;

class PGSQLBackup(object):
    """
    Exports all databases (if not in excluded list) as seperate
    backups into date folders. Settings are read from a json file,
    and pgsql account settings in a seperate file.
    """

    def __init__(self):
        """
        Initiate this class, set values and get
        settings from json file.
        """
        self.current_dir = os.getcwd()
        #self.current_dir = "/srv/tools/pgsqlbackup/"
        self.settings = self.get_settings()
        logging.basicConfig(filename=self.settings['log']['file'], level=logging.DEBUG)
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        self.backup_folder = os.path.join(self.settings['backup_path'], self.today)


    def get_settings(self):
        """
        Get settings from json file if the
        is one present.
        :return: list
        """
        settings_file_path = os.path.join(self.current_dir, 'settings.json')
        settings_file = open(settings_file_path)
        return json.load(settings_file)


    def ensure_folder_exists(self, folder):
        """
        Create folder and parent structure,
        if it doesnt exist.
        :param folder: String with the folder (full) path
        :return: bool
        """
        if os.path.isdir(folder):
            return True
        else:
            cmd = "mkdir -p {}".format(folder)
            try:
                check_call(xsplit(cmd))
                return True
            except CalledProcessError as error:
                logging.critical("Backup folder not present: %s", error)
                return False


    def get_db_list(self):
        """
        Get a list of databases.
        :return: List of databasenames or empty list
        """
        databases = []
        try:
            conn = psycopg2.connect(
                dbname=self.settings['pgsql']['default_db'],
                user=self.settings['pgsql']['user'],
                password=self.settings['pgsql']['password']
            )
            cur = conn.cursor()
            cur.execute("SELECT d.datname FROM pg_catalog.pg_database d ORDER BY 1;")
            rows = cur.fetchall()
            for row in rows:
                if row[0] not in self.settings['exclude']:
                    databases.append(row[0])
        except psycopg2.Error as error:
            logging.critical("PostgreSQL error: %s", error)
        except ValueError as error:
            logging.critical("Could not get a list of databases: %s", error)
        return databases


    def dump_databases(self, databases, folder):
        """
        Loop through a list if databases and dump them.
        :param databases: List with databases
        :param folder: String with the path to save them in
        :return: List with dumped databases of empty list
        """
        successfully_dumped = []
        for dbname in databases:
            if self.dump_database(dbname, folder):
                successfully_dumped.append(dbname)
            else:
                logging.error("Could not dump database: %s", dbname)
        return successfully_dumped

    def dump_database(self, dbname, folder):
        """
        Dump single database to backup file.
        This depends on the existens of a .pgpass file,
        see https://www.postgresql.org/docs/9.5/static/libpq-pgpass.html
        and https://stackoverflow.com/questions/2893954/how-to-pass-in-password-to-pg-dump
        :param dbname: String with database name
        :param folder: String with folder to save in
        :return: bool
        """
        # Put together the name of resulting SQL file
        sql_file = os.path.join(folder, dbname + '.sql')
        # Create command string for pgdump
        cmd_string = "{} -U {} {} > {}"
        cmd = cmd_string.format(
            self.settings['pg_dump']['bin'],
            self.settings['pgsql']['user'],
            dbname,
            sql_file
        )
        # Run command in pip
        process = subprocess.Popen(cmd, shell=True)
        # Wait for completion
        process.communicate()
        # Check for errors
        if process.returncode != 0:
            return False
        return True

    def delete_backup(self):
        """
        Delete the daily backup folder.
        :return: bool
        """
        cmd = "rm -rf {}".format(self.backup_folder)
        try:
            check_output(xsplit(cmd))
            return True
        except CalledProcessError as error:
            logging.warning("Could not delete backup folder: %s", error)
            return False

    def run(self):
        """
        Run the backup, dumping db to files.
        :return: list
        """
        if self.ensure_folder_exists(self.backup_folder):
            databases = self.get_db_list()
        if databases:
            dumped = self.dump_databases(databases, self.backup_folder)
        return dumped

def zip_folder(folder, files):
    """
    Create a zip file of todays backupfolder.
    :param dumped: list of dumped databases
    """
    try:
        zip_file = zipfile.ZipFile("{}.zip".format(folder), "w")
        for src_file in files:
            zip_file.write(
                "{}/{}.sql".format(folder, src_file),
                "{}.sql".format(src_file)
            )
        zip_file.close()
        return True
    except RuntimeError:
        return False


def main():
    """
    Create instance of the PGSQLBackup object
    and run it.
    """
    pgsql_backup = PGSQLBackup()
    databases = pgsql_backup.run()
    if zip_folder(pgsql_backup.backup_folder, databases):
        pgsql_backup.delete_backup()


if __name__ == "__main__":
    main()
