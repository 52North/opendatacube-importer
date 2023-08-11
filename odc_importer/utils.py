# Copyright (C) 2022-2023 52°North Spatial Information Research GmbH
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# If the program is linked with libraries which are licensed under one of
# the following licenses, the combination of the program with the linked
# library is not considered a "derivative work" of the program:
#
#     - Apache License, version 2.0
#     - Apache Software License, version 1.0
#     - GNU Lesser General Public License, version 3
#     - Mozilla Public License, versions 1.0, 1.1 and 2.0
#     - Common Development and Distribution License (CDDL), version 1.0
#
# Therefore the distribution of the program linked with libraries licensed
# under the aforementioned licenses, is permitted by the copyright holders
# if the distribution is compliant with both the GNU General Public
# License version 2 and the aforementioned licenses.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
from contextlib import closing
import hashlib
import logging
import os
import socket
import subprocess
import sys
import time
import zipfile

import psycopg2
import schedule

# logging_config_file = os.path.join(os.path.dirname(__file__), 'logging.yaml')
# level = logging.DEBUG
logger = logging.getLogger(__name__)


def verify_database_connection(db_name: str, db_host: str, db_port: int, db_user: str, db_password: str, no_ping: bool = False)\
        -> bool:
    if not no_ping:
        # ping host
        response = os.system("ping -c 1 {} > /dev/null".format(db_host))
        if response == 0:
            logger.info("Database host '{}' reachable via ping.".format(db_host))
        else:
            logger.error("Could not ping required database host '{}'.".format(db_host))
            return False
    # check host port
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        try:
            if sock.connect_ex((db_host, db_port)) == 0:
                logger.info("Database port '{}' on host '{}' is OPEN.".format(db_port, db_host))
            else:
                logger.error("Database port '{}' on host '{}' is CLOSED.".format(db_port, db_host))
                return False
        except socket.gaierror:
            logger.error("Hostname '{}' could not be resolved.".format(db_host))
            return False

    # psql connect
    with closing(psycopg2.connect("host={} port={} dbname={} user={} password={}"
                                  .format(db_host, db_port, db_name, db_user, db_password))) as db_conn:
        if db_conn and not db_conn.closed:
            logger.info("Database connection to '{}' established".format(db_name))
        else:
            logger.error("Could NOT connect to database '{}'".format(db_name))
            return False
    return True


def ensure_odc_connection_and_database_initialization(db_name: str, db_host: str, db_port: int, db_user: str,
                                                      db_pw: str, binary_home: str,
                                                      datacube_conf_file: str = 'datacube.conf') -> None:
    # Make sure that a datacube.conf file exists and the database schema is initialised
    #
    # https://datacube-core.readthedocs.io/en/latest/ops/config.html#configuration-via-environment-variables
    #
    with closing(open(datacube_conf_file, 'w')) as odc_config:
        odc_config.write("""[default]
db_database: {}
db_hostname: {}
db_port:     {}
db_username: {}
db_password: {}
index_driver: default
""".format(db_name, db_host, db_port, db_user, db_pw))

    # Check datacube database init status
    cmd = subprocess.Popen("{}datacube --config {} system check".format(binary_home, datacube_conf_file),
                           stdout=subprocess.PIPE, stderr=None, shell=True, bufsize=0)
    output = cmd.communicate()[0].decode()

    if cmd.returncode != 0:

        # database is not configured properly or not accessible
        if "Valid connection" in output and "Database not initialised" in output:

            logger.info("OpenDataCube database is not initialized. Doing it now...")

            cmd = subprocess.Popen("{}datacube --config {} system init".format(binary_home, datacube_conf_file),
                                   stdout=subprocess.PIPE, stderr=None, shell=True, bufsize=0)
            output = cmd.communicate()[0].decode()

            if cmd.returncode != 0:
                print("Any other error happened. Please check error output:\n{}".format(output))
                sys.exit(512)

            logger.info("OpenDataCube database is initialized.")

            # TODO process output
        else:

            print("Any other error happened. Please check error output:\n{}".format(output))
            sys.exit(256)


def check_global_data_folder(global_data_folder):

    if not os.path.exists(global_data_folder):
        logger.error("Global data folder '{}' not existing."
                     .format(global_data_folder))
    elif not os.access(global_data_folder, os.W_OK):
        logger.error("Global data folder '{}' exists but is not writable".format(global_data_folder))
        exit(32)
    logger.info("Ensured global data folder existence '{}'".format(global_data_folder))


def unzip(zip_file, out_folder):
    """
    Unzip zip file.
    :param zip_file: name of zip file to be unzipped
    :param out_folder: destination of unzipped content
    """

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(out_folder)


def calc_sha256(filename):
    """
    Calculate sha256 hash for a given file
    :param filename:
    :return: sha256 hash as string
    """

    sha256_hash = hashlib.sha256()
    with open(filename, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def run_periodic(func, every, unit, at=None, timezone='UTC', until=None, sleep=1):
    """
    Set up schedule and run <func> with this schedule.
    :param func: function
    :param every: int
    :param unit: str
    :param at: str
    :param timezone: str
    :param until: str
    :param sleep: int
    """
    job = None
    if unit.lower() == 'seconds':
        job = schedule.every(every).seconds
    if unit.lower() == 'minutes':
        job = schedule.every(every).minutes
    if unit.lower() == 'hours':
        job = schedule.every(every).hours
    if unit.lower() == 'days':
        job = schedule.every(every).days
    if unit.lower() == 'weeks':
        job = schedule.every(every).weeks
    if unit.lower() == 'monday':
        job = schedule.every().monday
    if unit.lower() == 'tuesday':
        job = schedule.every().tuesday
    if unit.lower() == 'wednesday':
        job = schedule.every().wednesday
    if unit.lower() == 'thursday':
        job = schedule.every().thursday
    if unit.lower() == 'friday':
        job = schedule.every().friday
    if unit.lower() == 'saturday':
        job = schedule.every().saturday
    if unit.lower() == 'sunday':
        job = schedule.every().sunday

    if not job:
        logger.error(f"Periodic mode chosen, but schedule job could not be created."
                     f"Check the environment variables PERIODIC_EVERY and PERIODIC_UNIT.")
        exit(1)

    msg = f"{every} {unit}"
    if at:
        job = job.at(at, timezone)
        msg += f" at {at}, {timezone},"
    if until:
        job = job.until(until)
        msg += f" until {until}"
    job.do(func)

    logger.info(f"""
    Periodic mode chosen
    ---------------
    Open Data Cube importer runs every {msg}.
    Sleeping time between runs at least: {sleep} s.
    """)

    while True:
        schedule.run_pending()
        time.sleep(sleep)
