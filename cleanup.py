#!/usr/bin/env python

import sys

import requests
import click

def _cleanup_objects(site, endpoint, auth=None):

    list_path = "{:s}/{:s}/".format(site, endpoint)

    r = requests.get(list_path, auth=auth)
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
        object_path = "{:s}/{:s}/{:s}/".format(site, endpoint, object_uuid)
        r = requests.delete(object_path, auth=auth)
        r.raise_for_status()

    elif (i == len(object_uuids)):
        errors = []
        with click.progressbar(object_uuids,
                               label='Deleting {:s}'.format(endpoint),
                               item_show_func=str) as bar:
            for object_uuid in bar:
                object_path = "{:s}/{:s}/{:s}/".format(site, endpoint, object_uuid)
                r = requests.delete(object_path, auth=auth)
                try:
                    r.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    errors.append([object_uuid, e, r.json()])
        if errors:
            print("Errors:")
            for error in errors:
                msg = None
                if error[2]:
                    if 'message' in error[2]:
                        msg = error[2]['message']
                print("{:s}: {:s}: {:s}".format(error[0], str(error[1]), str(msg)))

    else:
        raise Exception("Selection out of range")

@click.command()
@click.argument('url')
@click.option('--username', default=None, help='API Username')
@click.option('--password', default=None, help='API Password')
@click.option('--token', default=None, help='API Token')
def cleanup(url, username, password, token):
    """COG CLI"""

    if token:
        auth = requests.auth.HTTPBasicAuth(token, '')
    elif username:
        if not password:
            password = raw_input("{:s} Password: ".format(username))
        auth = requests.auth.HTTPBasicAuth(username, password)
    else:
        auth = None

    _cleanup_objects(url, 'assignments', auth)
    _cleanup_objects(url, 'tests', auth)
    _cleanup_objects(url, 'submissions', auth)
    _cleanup_objects(url, 'runs', auth)
    _cleanup_objects(url, 'reporters', auth)
    _cleanup_objects(url, 'files', auth)

if __name__ == '__main__':
    sys.exit(cleanup())
