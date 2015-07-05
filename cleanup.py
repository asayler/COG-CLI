#!/usr/bin/env python

import sys

import requests
import click

SITE = "https://api-cog.cs.colorado.edu"

def _delete_object(endpoint, object_uuid, auth=None):

    object_path = "{:s}/{:s}/{:s}/".format(SITE, endpoint, object_uuid)

    print("Deleting {:s} '{:s}'...".format(endpoint, object_uuid))
    r = requests.delete(object_path, auth=auth)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        raise
    else:
        print("Deleted {:s} '{:s}'".format(endpoint, r.json().keys()[0]))

def _cleanup_objects(endpoint, auth=None):

    path = "{:s}/{:s}/".format(SITE, endpoint)

    r = requests.get(path, auth=auth)
    r.raise_for_status()

    object_uuids = r.json()[endpoint]

    if len(object_uuids) == 0:
        print("No {:s} to delete, skipping...".format(endpoint))
        return

    i = 0
    for object_uuid in object_uuids:
        print("{:6d}: {:s}".format(i, object_uuid))
        i += 1
    print("{:6d}: {:s}".format(i, "All"))
    i = int(input("Select {:s} to delete: ".format(endpoint)))

    if (i < len(object_uuids)):
        object_uuid = object_uuids[i]
        _delete_object(endpoint, object_uuid, auth=auth)
    elif (i == len(object_uuids)):
        for object_uuid in object_uuids:
            _delete_object(endpoint, object_uuid, auth=auth)
    else:
        raise Exception("Selection out of range")

@click.command()
@click.option('--username', default=None, help='API Username')
@click.option('--password', default=None, help='API Password')
@click.option('--token', default=None, help='API Token')
def cleanup(username, password, token):
    """COG CLI"""

    if token:
        auth = requests.auth.HTTPBasicAuth(token, '')
    elif username:
        if not password:
            password = raw_input("{:s} Password: ".format(username))
        auth = requests.auth.HTTPBasicAuth(username, password)
    else:
        auth = None

    _cleanup_objects('assignments', auth)
    _cleanup_objects('tests', auth)
    _cleanup_objects('submissions', auth)
    _cleanup_objects('runs', auth)
    _cleanup_objects('reporters', auth)
    _cleanup_objects('files', auth)

if __name__ == '__main__':
    sys.exit(cleanup())
