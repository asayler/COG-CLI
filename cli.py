#!/usr/bin/env python3

import sys
import json

import requests
import click

import client

_EP_TOKENS = 'tokens'

def _auth(obj):

    # Handle Auth
    endpoint = "{:s}/{:s}/".format(obj['url'], _EP_TOKENS)

    if obj['token']:

        # Verify Token
        auth = requests.auth.HTTPBasicAuth(obj['token'], '')
        r = requests.get(endpoint, auth=auth)
        r.raise_for_status()
        obj['token'] = r.json()['token']
        auth = requests.auth.HTTPBasicAuth(obj['token'], '')

    else:

        # Get Username:Password
        if not obj['username']:
            obj['username'] = click.prompt("Username", hide_input=False)
        if not obj['password']:
            obj['password'] = click.prompt("Password", hide_input=True)

        # Get Token
        auth = requests.auth.HTTPBasicAuth(obj['username'], obj['password'])
        r = requests.get(endpoint, auth=auth)
        r.raise_for_status()
        obj['token'] = r.json()['token']
        auth = requests.auth.HTTPBasicAuth(obj['token'], '')

    return auth

@click.group()
@click.option('--url', default=None, prompt=True, help='API URL')
@click.option('--username', default=None, help='API Username')
@click.option('--password', default=None, help='API Password')
@click.option('--token', default=None, help='API Token')
@click.pass_context
def cli(ctx, url, username, password, token):
    """COG CLI"""

    # Setup Context
    ctx.obj = {}
    ctx.obj['url'] = url
    ctx.obj['username'] = username
    ctx.obj['password'] = password
    ctx.obj['token'] = token
    ctx.obj['auth'] = None

@cli.group()
def assignment():
    pass

@assignment.command(name='create')
@click.option('--name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default=None, prompt=True, help='Assignment Environment')
@click.pass_obj
def assignment_create(obj, name, env):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Creating assignment...")
    asn_list = client.assignment_create(obj['url'], obj['auth'], name, env)
    click.echo("Created assignments:\n {}".format(asn_list))

@assignment.command(name='list')
@click.pass_obj
def assignment_list(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Listing assignments")
    asn_list = client.assignment_list(obj['url'], obj['auth'])
    click.echo("Assignments:\n {}".format(asn_list))

@assignment.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
def assignment_show(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Showing assignment...")
    asn = client.assignment_show(obj['url'], obj['auth'], uid)
    click.echo("Assignment '{}':\n {}".format(uid, asn))

@cli.group()
def test():
    pass

@test.command(name='create')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.option('--name', default=None, prompt=True, help='Test Name')
@click.option('--tester', default=None, prompt=True, help='Test Module')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.pass_obj
def test_create(obj, asn_uid, name, tester, maxscore):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Creating test...")
    tst_list = client.assignment_test_create(obj['url'], obj['auth'], asn_uid, name, tester, maxscore)
    click.echo("Created tests:\n {}".format(tst_list))

@cli.group()
def file():
    pass

@file.command(name='create')
@click.option('--path', default=None, prompt=True, type=click.File('rb'), help='File Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.pass_obj
def file_create(obj, path, extract):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Creating file...")
    fle_list = client.file_create(obj['url'], obj['auth'], path, extract)
    click.echo("Files:\n {}".format(fle_list))

@file.command(name='list')
@click.option('--tst_uid', default=None, help='Only Show Files from Test with UUID tst_uid')
@click.pass_obj
def file_list(obj, tst_uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Listing files...")
    fle_list = client.file_list(obj['url'], obj['auth'], tst_uid=tst_uid)
    click.echo("Files:\n {}".format(fle_list))

@file.command(name='show')
@click.option('--uid', default=None, prompt=True, help='File UUID')
@click.pass_obj
def file_show(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Showing file...")
    fle = client.file_show(obj['url'], obj['auth'], uid)
    click.echo("File '{}':\n {}".format(uid, fle))

@file.command(name='show')
@click.option('--uid', default=None, prompt=True, help='File UUID')
@click.pass_obj
def file_delete(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Deleting file...")
    fle = client.file_delete(obj['url'], obj['auth'], uid)
    click.echo("Deleted file '{}':\n {}".format(uid, fle))

@cli.group()
def util():
    pass

@util.command(name='replace-files')
@click.option('--path', default=None, prompt=True, type=click.File('rb'), help='File Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--tst_uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
def util_replace_files(obj, path, extract, tst_uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Listing old files...")
    old_fle_list = client.file_list(obj['url'], obj['auth'], tst_uid=tst_uid)
    click.echo("Old files:\n {}".format(old_fle_list))

    click.echo("Removing old files...")
    rem_fle_list = client.test_file_remove(obj['url'], obj['auth'], tst_uid, old_fle_list)
    click.echo("Attached files:\n {}".format(rem_fle_list))

    click.echo("Deleting old files...")
    for fle_uid in old_fle_list:
        click.echo("Deleting file '{}'...".format(fle_uid))
        fle = client.file_delete(obj['url'], obj['auth'], fle_uid)

    click.echo("Creating new files...")
    new_fle_list = client.file_create(obj['url'], obj['auth'], path, extract)
    click.echo("New files:\n {}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = client.test_file_add(obj['url'], obj['auth'], tst_uid, new_fle_list)
    click.echo("Attached files:\n {}".format(tst_fle_list))

@util.command(name='token-show')
@click.pass_obj
def util_token_show(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("{} Token: '{}'".format(obj['username'], obj['token']))

@util.command(name='setup-assignment')
@click.option('--asn_name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default=None, prompt=True, help='Assignemnt Environment')
@click.option('--tst_name', default=None, prompt=True, help='Test Name')
@click.option('--tester', default=None, prompt=True, help='Test Module')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.option('--path', default=None, prompt=True, type=click.File('rb'), help='File Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.pass_obj
def setup_assignment(obj, asn_name, env, tst_name, tester, maxscore, path, extract):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Creating assignment...")
    asn_list = client.assignment_create(obj['url'], obj['auth'], asn_name, env)
    click.echo("Created assignments:\n {}".format(asn_list))
    asn_uid = asn_list[0]

    click.echo("Creating test...")
    tst_list = client.assignment_test_create(obj['url'], obj['auth'], asn_uid, tst_name, tester, maxscore)
    click.echo("Created tests:\n {}".format(tst_list))
    tst_uid = tst_list[0]

    click.echo("Creating files...")
    new_fle_list = client.file_create(obj['url'], obj['auth'], path, extract)
    click.echo("Created files:\n {}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = client.test_file_add(obj['url'], obj['auth'], tst_uid, new_fle_list)
    click.echo("Attached files:\n {}".format(tst_fle_list))

if __name__ == '__main__':
    sys.exit(cli())
