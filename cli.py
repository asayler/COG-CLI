#!/usr/bin/env python3

import sys
import json

import requests
import click

_EP_ASSIGNMENTS = 'assignments'
_KEY_ASSIGNMENTS = 'assignments'
_EP_TOKENS = 'tokens'

def _assignment_create(url, auth, assn_name, assn_env):

    endpoint = "{:s}/{:s}/".format(url, _EP_ASSIGNMENTS)
    d = {'name': assn_name, 'env': assn_env}
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

def _auth(url, username=None, password=None, token=None):

    # Handle Auth
    endpoint = "{:s}/{:s}".format(url, _EP_TOKENS)

    if token:

        # Verify Token
        auth = requests.auth.HTTPBasicAuth(username, password)
        r = requests.get(endpoint, auth=auth)
        r.raise_for_status()
        token = r.json()['token']
        auth = requests.auth.HTTPBasicAuth(token, '')

    else:

        # Get Username:Password
        if not username:
            username = click.prompt("Username", hide_input=False)
        if not password:
            password = click.prompt("Password", hide_input=True)

        # Get Token
        auth = requests.auth.HTTPBasicAuth(username, password)
        r = requests.get(endpoint, auth=auth)
        r.raise_for_status()
        token = r.json()['token']
        auth = requests.auth.HTTPBasicAuth(token, '')

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
        obj['auth'] = _auth(obj['url'], obj['username'], obj['password'], obj['token'])

    asn_list = _assignment_create(obj['url'], obj['auth'], name, env)
    click.echo("Assignments:\n {}".format(asn_list))

@assignment.command(name='list')
@click.pass_obj
def assignment_list(obj):

    if not obj['auth']:
        obj['auth'] = _auth(obj['url'], obj['username'], obj['password'], obj['token'])

    asn_list = _assignment_list(obj['url'], obj['auth'])
    click.echo("Assignments:\n {}".format(asn_list))

@assignment.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
def assignment_show(obj, uid):

    if not obj['auth']:
        obj['auth'] = _auth(obj['url'], obj['username'], obj['password'], obj['token'])

    asn = _assignment_show(obj['url'], obj['auth'], uid)
    click.echo("Assignment '{}':\n {}".format(uid, asn))

if __name__ == '__main__':
    sys.exit(cli())
