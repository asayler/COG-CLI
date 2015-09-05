#!/usr/bin/env python3

import sys
import json

import requests
import click

_EP_TOKENS = 'tokens'
_EP_ASSIGNMENTS = 'assignments'
_KEY_ASSIGNMENTS = 'assignments'
_EP_TESTS = 'tests'
_KEY_TESTS = 'tests'
_EP_FILES = 'files'
_KEY_FILES = 'files'

def _debug_dump(r):

    click.echo('Request:\n{}\n{}\n{}\n\n{}\n{}'.format(
        '-----------START-----------',
        r.request.method + ' ' + r.request.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in r.request.headers.items()),
        r.request.body,
        '------------END------------'
    )
    )
    click.echo('Response:\n{}'.format(r.text))

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

def _assignment_create(url, auth, asn_name, asn_env):

    endpoint = "{:s}/{:s}/".format(url, _EP_ASSIGNMENTS)
    d = {'name': asn_name, 'env': asn_env}
    dj = json.dumps(d)
    r = requests.post(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    asn_list = r.json()[_KEY_ASSIGNMENTS]
    return asn_list

def _assignment_list(url, auth):

    endpoint = "{:s}/{:s}/".format(url, _EP_ASSIGNMENTS)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    asn_list = r.json()[_KEY_ASSIGNMENTS]
    return asn_list

def _assignment_show(url, auth, uid):

    endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_ASSIGNMENTS, uid)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    asn = r.json()[uid]
    return asn

def _assignment_test_create(url, auth, asn_uid, tst_name, tst_tester, tst_maxscore):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_ASSIGNMENTS, asn_uid, _EP_TESTS)
    d = {"name": tst_name, "tester": tst_tester, "maxscore": tst_maxscore}
    dj = json.dumps(d)
    r = requests.post(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    tst_list = r.json()[_KEY_TESTS]
    return tst_list

def _test_file_add(url, auth, tst_uid, fle_uids):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
    d = {_KEY_FILES: fle_uids}
    dj = json.dumps(d)
    r = requests.put(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def _test_file_remove(url, auth, tst_uid, fle_uids):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
    d = {_KEY_FILES: fle_uids}
    dj = json.dumps(d)
    r = requests.delete(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def _file_create(url, auth, path, extract=False):

    if extract:
        key = 'extract'
    else:
        key = 'file'

    endpoint = "{:s}/{:s}/".format(url, _EP_FILES)
    files = {key: path}
    r = requests.post(endpoint, files=files, auth=auth)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def _file_list(url, auth):

    endpoint = "{:s}/{:s}/".format(url, _EP_FILES)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def _file_show(url, auth):

    endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_FILES, uid)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    fle = r.json()[uid]
    return fle

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
    asn_list = _assignment_create(obj['url'], obj['auth'], name, env)
    click.echo("Assignments:\n {}".format(asn_list))

@assignment.command(name='list')
@click.pass_obj
def assignment_list(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Listing assignments")
    asn_list = _assignment_list(obj['url'], obj['auth'])
    click.echo("Assignments:\n {}".format(asn_list))

@assignment.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
def assignment_show(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Showing assignment...")
    asn = _assignment_show(obj['url'], obj['auth'], uid)
    click.echo("Assignment '{}':\n {}".format(uid, asn))

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
    fle_list = _file_create(obj['url'], obj['auth'], path, extract)
    click.echo("Files:\n {}".format(fle_list))

@file.command(name='list')
@click.pass_obj
def file_list(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Listing files...")
    fle_list = _file_list(obj['url'], obj['auth'])
    click.echo("Files:\n {}".format(fle_list))

@file.command(name='show')
@click.option('--uid', default=None, prompt=True, help='File UUID')
@click.pass_obj
def file_show(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Showing file...")
    fle = _file_show(obj['url'], obj['auth'], uid)
    click.echo("File '{}':\n {}".format(uid, fle))

@cli.group()
def util():
    pass

@util.command(name='replace_files')
@click.option('--path', default=None, prompt=True, type=click.File('rb'), help='File Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
def util_repalce_files(obj, path, extract, asn_uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("Creating files...")
    fle_list = _file_create(obj['url'], obj['auth'], path, extract)
    click.echo("Created Files:\n {}".format(fle_list))

    click.echo("Attaching files...")
    asn_fle_list = _assignment_file_add(obj['url'], obj['auth'], asn_uid, fle_list)
    click.echo("Attached Files:\n {}".format(asn_fle_list))

@util.command(name='token-show')
@click.pass_obj
def util_token_show(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj)

    click.echo("{} Token: '{}'".format(obj['username'], obj['token']))

if __name__ == '__main__':
    sys.exit(cli())
