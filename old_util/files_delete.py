#!/usr/bin/env python

import sys

import requests
import click

SITE = "https://api-cog.cs.colorado.edu"

def _delete_file(file_uuid, auth=None):

    endpoint = "files"
    files_path = "{:s}/{:s}/".format(SITE, endpoint)

    print("Deleteing file '{:s}'...".format(file_uuid))
    file_path = "{:s}{:s}/".format(files_path, file_uuid)
    r = requests.delete(file_path, auth=auth)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        return 1
    else:
        print("Deleted file '{:s}'".format(r.json().keys()[0]))


@click.command()
@click.option('--username', default=None, help='API Username')
@click.option('--password', default=None, help='API Password')
@click.option('--token', default=None, help='API Token')
def delete_files(username, password, token):
    """COG CLI"""

    endpoint = "files"
    files_path = "{:s}/{:s}/".format(SITE, endpoint)

    if token:
        auth = requests.auth.HTTPBasicAuth(token, '')
    elif username:
        if not password:
            password = raw_input("{:s} Password: ".format(username))
        auth = requests.auth.HTTPBasicAuth(username, password)
    else:
        auth = None

    r = requests.get(files_path, auth=auth)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        return 1

    file_uuids = r.json()['files']

    i = 0
    for file_uuid in file_uuids:
        print("{:3d}: {:s}".format(i, file_uuid))
        i += 1
    print("{:3d}: {:s}".format(i, "All"))
    i = int(input("Select file to delete: "))

    if (i < len(file_uuids)):
        file_uuid = file_uuids[i]
        _delete_file(file_uuid, auth=auth)
    elif (i == len(file_uuids)):
        print("Deleteing all files...")
        for file_uuid in file_uuids:
            _delete_file(file_uuid, auth=auth)
    else:
        print("Selection out of range")
        return 1


if __name__ == '__main__':
    sys.exit(delete_files())
