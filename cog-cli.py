#!/usr/bin/env python3


import sys
import json
import functools
import os
import os.path
import time
import uuid
import threading
import concurrent.futures
import queue
import datetime
import configparser

import requests
import click

import api_client
import util_click
import util_cli


_APP_NAME = 'cog-cli'
_PATH_SERVER_CONF = os.path.join(click.get_app_dir(_APP_NAME), 'servers')


### Auth Functions ###

def auth_required(func):

    @functools.wraps(func)
    def _wrapper(obj, *args, **kwargs):

        if not obj['connection'].is_authenticated():

            # Token
            if obj['token']:
                obj['connection'].authenticate(token=obj['token'])

            # Username:Password
            else:
                if not obj['username']:
                    obj['username'] = click.prompt("Username", hide_input=False)
                if not obj['password']:
                    obj['password'] = click.prompt("Password", hide_input=True)
                obj['connection'].authenticate(username=obj['username'],
                                               password=obj['password'])

        # Call Function
        return func(obj, *args, **kwargs)

    return _wrapper


### Async Helper Functions ###

def async_obj_map(obj_list, async_fun,
                  async_func_args=[], async_func_kwargs={},
                  label=None, timing=False, sleep=0.1):

    if timing:
        start = time.time()

    future = {}
    for key in obj_list:
        future[key] = async_fun(key, *async_func_args, **async_func_kwargs)

    output = {}
    failed = {}
    with click.progressbar(label=label, length=len(future)) as bar:
        while future:
            remain = future
            future = {}
            for key, f in remain.items():
                if f.done():
                    try:
                        output[key] = f.result()
                    except Exception as err:
                        failed[key] = err
                    finally:
                        bar.update(1)
                else:
                    future[key] = f
            time.sleep(sleep)

    if timing:
        end = time.time()
        dur = end - start
        dur_str = "Dur: {}".format(util_cli.duration_to_str(dur))
        ops = len(obj_list)/dur
        ops_str = "Objs/sec: {:6.0f}".format(ops)
        offset = "{val:{width}s}".format(val="", width=(len(label)+1))
        click.echo("{}  {},   {}".format(offset, dur_str, ops_str))

    return output, failed

def async_obj_fetch(iter_parent, obj_name=None, obj_client=None,
                    async_list=None, async_show=None, timing=False,
                    prefilter_list=None, prefilter_func=None,
                    prefilter_func_args=[], prefilter_func_kwargs={},
                    postfilter_list=None, postfilter_func=None,
                    postfilter_func_args=[], postfilter_func_kwargs={}):

    # Async List
    if async_list is None:
        if obj_client is not None:
            async_list = obj_client.async_list()
        else:
            raise TypeError("Requires either obj_client or async_list")
    label = "Listing  {}".format(obj_name if obj_name else "")
    lists, lists_failed = async_obj_map(iter_parent, async_list,
                                        label=label, timing=timing)
    todo_set = lists_to_set(lists)

    # Pre-Filter List
    if prefilter_list:
        todo_set_orig = todo_set
        todo_set = set()
        for ouid in prefilter_list:
            if ouid in todo_set_orig:
                todo_set.add(ouid)
            else:
                obj_str = obj_name if obj_name else "object"
                msg = "Pre-filtered {} '{}' not found in '{}'".format(obj_str, ouid, todo_set_orig)
                raise TypeError(msg)

    # Pre-Filter Function
    if prefilter_func:
        todo_set_orig = todo_set
        todo_set = set()
        for ouid in todo_set_orig:
            if prefilter_func(ouid, *prefilter_func_args, **prefilter_func_kwargs):
                todo_set.add(ouid)

    # Async Get
    if async_show is None:
        if obj_client is not None:
            async_show = obj_client.async_show()
        else:
            raise TypeError("Requires either obj_clientn ot async_show")
    label = "Getting  {}".format(obj_name if obj_name else "")
    objs, objs_failed = async_obj_map(todo_set, async_show,
                                      label=label, timing=timing)

    # Post-Filter List
    if postfilter_list:
        objs_orig = objs
        objs = {}
        for ouid in postfilter_list:
            if ouid in objs_orig:
                objs[ouid] = objs_orig[ouid]
            else:
                obj_str = obj_name if obj_name else "object"
                msg = "Post-filtered {} '{}' not found".format(obj_str, ouid)
                raise TypeError(msg)

    # Post-Filter Function
    if postfilter_func:
        objs_orig = objs
        objs = {}
        for ouid, obj in objs_orig.items():
            if postfilter_func(ouid, obj, *postfilter_func_args, **postfilter_func_kwargs):
                objs[ouid] = obj

    # Return
    return lists, todo_set, objs, lists_failed, objs_failed

def lists_to_set(lists):

    sset = set([ouid for puid, ouids in lists.items() for ouid in ouids])
    return sset


### Post Processing Filters ###

def postfilter_attr_owner(ouid, obj, owners):

    if owners:
        return uuid.UUID(obj['owner']) in owners
    else:
        return True

def postfilter_attr_test(ouid, obj, tests):

    if tests:
        return uuid.UUID(obj['test']) in tests
    else:
        return True


### CLI Root ###

@click.group()
@click.option('--server', default=None, help="API Server (from [config_path])")
@click.option('--url', default=None, help="API URL")
@click.option('--username', default=None, help="API Username")
@click.option('--password', default=None, help="API Password")
@click.option('--token', default=None, help="API Token")
@click.option('--conf_path', default=_PATH_SERVER_CONF, prompt=False,
              type=click.Path(resolve_path=True),
              help="Config Path ('{}')".format(_PATH_SERVER_CONF))
@click.pass_context
def cli(ctx, server, url, username, password, token, conf_path):
    """COG CLI"""

    # Read Config
    if server is not None:
        if not os.path.isfile(conf_path):
            raise click.FileError(conf_path)
        conf_obj = configparser.ConfigParser()
        conf_obj.read(conf_path)
        if not server in conf_obj:
            msg = "Could not find '{}' in '{}'".format(server, conf_path)
            raise click.BadOptionUsage('server', msg)
        conf_dict = conf_obj[server]
        if url is None:
            url = conf_dict['url']
        if username is None:
            username = conf_dict['user']
        if password is None:
            if token is None:
                token = conf_dict['token']

    # Check Required Parameters
    if not url:
        raise click.UsageError("URL required")

    # Setup Context
    ctx.obj = {}
    ctx.obj['url'] = url
    ctx.obj['username'] = username
    ctx.obj['password'] = password
    ctx.obj['token'] = token
    ctx.obj['connection'] = api_client.AsyncConnection(ctx.obj['url'])


### My Commands ###

@cli.group(name='my')
@click.pass_obj
def my(obj):

    # Setup Client Class
    obj['myinfo'] = api_client.MyInfo(obj['connection'])

@my.command(name='token')
@click.pass_obj
@auth_required
def my_token(obj):

    token = obj['myinfo'].token()
    click.echo("{}".format(token))

@my.command(name='username')
@click.pass_obj
@auth_required
def my_username(obj):

    username = obj['myinfo'].username()
    click.echo("{}".format(username))

@my.command(name='useruuid')
@click.pass_obj
@auth_required
def my_useruuid(obj):

    useruuid = obj['myinfo'].useruuid()
    click.echo("{}".format(useruuid))


### File Commands ###

@cli.group(name='file')
@click.pass_obj
def fle(obj):

    # Setup Client Class
    obj['files'] = api_client.Files(obj['connection'])

@fle.command(name='create')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.pass_obj
@auth_required
def fle_create(obj, path, extract):

    fle_list = obj['files'].create(path, extract)
    click.echo("{}".format(fle_list))

@fle.command(name='list')
@click.option('--tst_uid', default=None, type=click.UUID,
              help='Only list files attached to a specific test')
@click.option('--sub_uid', default=None, type=click.UUID,
              help='Only list files attached to a specific submission')
@click.pass_obj
@auth_required
def fle_list(obj, tst_uid, sub_uid):

    fle_list = obj['files'].list(tst_uid=tst_uid, sub_uid=sub_uid)
    click.echo("{}".format(fle_list))

@fle.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def fle_show(obj, uid):

    fle = obj['files'].show(uid)
    click.echo("{}".format(fle))

@fle.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def fle_delete(obj, uid):

    fle = obj['files'].delete(uid)
    click.echo("{}".format(fle))

@fle.command(name='download')
@click.option('--uid', prompt=True, type=click.UUID, help='File UUID')
@click.option('--path', default=None, prompt=True,
              type=click.Path(writable=True, resolve_path=True),
              help='Destination Path')
@click.option('--orig_path', is_flag=True,
              help='Control whether original path is used')
@click.pass_obj
@auth_required
def fle_download(obj, uid, path, orig_path):

    path = obj['files'].download(uid, path, orig_path=orig_path)
    click.echo("{}".format(path))


### Assignment Commands ###

@cli.group()
@click.pass_obj
def assignment(obj):

    # Setup Client Class
    obj['assignments'] = api_client.Assignments(obj['connection'])

@assignment.command(name='create')
@click.option('--name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default='local', help='Assignment Environment')
@click.option('--duedate', default=None, help='Assignment Due Date')
@click.option('--respect_duedate', default=None, help='Respect Assignment Due Date')
@click.option('--accepting_runs', default=False, help='Assignment Accepting Test Runs')
@click.option('--accepting_subs', default=False, help='Assignment Accepting Submissions')
@click.pass_obj
@auth_required
def assignment_create(obj, name, env, duedate, respect_duedate,
                      accepting_runs, accepting_subs):


    asn_list = obj['assignments'].create(name, env=env,
                                         duedate=duedate,
                                         respect_duedate=respect_duedate,
                                         accepting_runs=accepting_runs,
                                         accepting_subs=accepting_subs)
    click.echo("{}".format(asn_list))

@assignment.command(name='update')
@click.option('--uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.option('--name', default=None, help='Assignment Name')
@click.option('--env', default=None, help='Assignment Environment')
@click.option('--duedate', default=None, help='Assignment Due Date')
@click.option('--respect_duedate', default=None, help='Respect Assignment Due Date')
@click.option('--accepting_runs', default=None, help='Assignment Accepting Test Runs')
@click.option('--accepting_subs', default=None, help='Assignment Accepting Submissions')
@click.pass_obj
@auth_required
def assignment_update(obj, uid, name, env, duedate, respect_duedate,
                      accepting_runs, accepting_subs):

    asn = obj['assignments'].update(uid, name=name, env=env,
                                         duedate=duedate,
                                         respect_duedate=respect_duedate,
                                         accepting_runs=accepting_runs,
                                         accepting_subs=accepting_subs)
    click.echo("{}".format(asn))

@assignment.command(name='list')
@click.option('--submitable', is_flag=True, help='Limit to submitable assignments')
@click.option('--runable', is_flag=True, help='Limit to runable assignments')
@click.pass_obj
@auth_required
def assignment_list(obj, submitable, runable):

    asn_list = obj['assignments'].list(submitable=submitable, runable=runable)
    click.echo("{}".format(asn_list))

@assignment.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_show(obj, uid):

    asn = obj['assignments'].show(uid)
    click.echo("{}".format(asn))

@assignment.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_delete(obj, uid):

    asn = obj['assignments'].delete(uid)
    click.echo("{}".format(asn))

@assignment.command(name='activate')
@click.option('--uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_activate(obj, uid):

    asn = obj['assignments'].update(uid, accepting_runs=True, accepting_subs=True)
    click.echo("{}".format(asn))

@assignment.command(name='deactivate')
@click.option('--uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_deactivate(obj, uid):

    asn = obj['assignments'].update(uid, accepting_runs=False, accepting_subs=False)
    click.echo("{}".format(asn))


### Test Commands ###

@cli.group()
@click.pass_obj
def test(obj):

    # Setup Client Class
    obj['tests'] = api_client.Tests(obj['connection'])

@test.command(name='create')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.option('--name', default=None, prompt=True, help='Test Name')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.option('--tester', default='script', help='Test Module')
@click.option('--builder', default=None, help='Build Module')
@click.option('--path_script', default=None, help='Relative Path to Grading Script')
@click.pass_obj
@auth_required
def test_create(obj, asn_uid, name, maxscore, tester, builder, path_script):

    tst_list = obj['tests'].create(asn_uid, name, maxscore,
                                   tester=tester, builder=builder,
                                   path_script=path_script)
    click.echo("{}".format(tst_list))

@test.command(name='update')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.option('--name', default=None, help='Test Name')
@click.option('--maxscore', default=None, help='Max Score')
@click.option('--tester', default=None, help='Test Module')
@click.option('--builder', default=None, help='Build Module')
@click.option('--path_script', default=None, help='Relative Path to Grading Script')
@click.pass_obj
@auth_required
def test_update(obj, uid, name, maxscore, tester, builder, path_script):

    tst = obj['tests'].update(uid, name=name, maxscore=maxscore,
                              tester=tester, builder=builder,
                              path_script=path_script)
    click.echo("{}".format(tst))

@test.command(name='list')
@click.option('--asn_uid', default=None, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def test_list(obj, asn_uid):

    tst_list = obj['tests'].list(asn_uid=asn_uid)
    click.echo("{}".format(tst_list))

@test.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def test_show(obj, uid):

    tst = obj['tests'].show(uid)
    click.echo("{}".format(tst))

@test.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def test_delete(obj, uid):

    tst = obj['tests'].delete(uid)
    click.echo("{}".format(tst))

@test.command(name='attach_files')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.option('--fle_uid', multiple=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def test_attach_files(obj, uid, fle_uid):

    tst = obj['tests'].attach_files(uid, fle_uid)
    click.echo("{}".format(tst))

@test.command(name='detach_files')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.option('--fle_uid', multiple=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def test_detach_files(obj, uid, fle_uid):

    tst = obj['tests'].detach_files(uid, fle_uid)
    click.echo("{}".format(tst))

@test.command(name='attach_reporters')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.option('--rpt_uid', multiple=True, type=click.UUID, help='Reporter UUID')
@click.pass_obj
@auth_required
def test_attach_reporters(obj, uid, rpt_uid):

    tst = obj['tests'].attach_reporters(uid, rpt_uid)
    click.echo("{}".format(tst))

@test.command(name='detach_reporters')
@click.option('--uid', prompt=True, type=click.UUID, help='Test UUID')
@click.option('--rpt_uid', multiple=True, type=click.UUID, help='Reporter UUID')
@click.pass_obj
@auth_required
def test_detach_reporters(obj, uid, fle_uid):

    tst = obj['tests'].detach_reporters(uid, rpt_uid)
    click.echo("{}".format(tst))


### Submission Commands ###

@cli.group()
@click.pass_obj
def submission(obj):

    # Setup Client Class
    obj['submissions'] = api_client.Submissions(obj['connection'])

@submission.command(name='create')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
@auth_required
def submission_create(obj, asn_uid):

    tst_list = obj['submissions'].create(asn_uid)
    click.echo("{}".format(tst_list))

@submission.command(name='list')
@click.option('--asn_uid', default=None, type=click.UUID, help='Assignment UUID')
@click.pass_obj
@auth_required
def submission_list(obj, asn_uid):

    tst_list = obj['submissions'].list(asn_uid=asn_uid)
    click.echo("{}".format(tst_list))

@submission.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='Submission UUID')
@click.pass_obj
@auth_required
def submission_show(obj, uid):

    tst = obj['submissions'].show(uid)
    click.echo("{}".format(tst))

@submission.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='Submission UUID')
@click.pass_obj
@auth_required
def submission_delete(obj, uid):

    tst = obj['submissions'].delete(uid)
    click.echo("{}".format(tst))

@submission.command(name='attach_files')
@click.option('--uid', prompt=True, type=click.UUID, help='Submission UUID')
@click.option('--fle_uid', multiple=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def submission_attach_files(obj, uid, fle_uid):

    tst = obj['submissions'].attach_files(uid, fle_uid)
    click.echo("{}".format(tst))

@submission.command(name='detach_files')
@click.option('--uid', prompt=True, type=click.UUID, help='Submission UUID')
@click.option('--fle_uid', multiple=True, type=click.UUID, help='File UUID')
@click.pass_obj
@auth_required
def submission_detach_files(obj, uid, fle_uid):

    tst = obj['submissions'].detach_files(uid, fle_uid)
    click.echo("{}".format(tst))


### Run Commands ###

@cli.group()
@click.pass_obj
def run(obj):

    # Setup Client Class
    obj['runs'] = api_client.Runs(obj['connection'])

@run.command(name='create')
@click.option('--sub_uid', prompt=True, type=click.UUID, help='Submission UUID')
@click.option('--tst_uid', prompt=True, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def run_create(obj, sub_uid, tst_uid):

    tst_list = obj['runs'].create(sub_uid, tst_uid)
    click.echo("{}".format(tst_list))

@run.command(name='list')
@click.option('--sub_uid', default=None, type=click.UUID, help='Submission UUID')
@click.pass_obj
@auth_required
def run_list(obj, sub_uid):

    tst_list = obj['runs'].list(sub_uid=sub_uid)
    click.echo("{}".format(tst_list))

@run.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='Run UUID')
@click.pass_obj
@auth_required
def run_show(obj, uid):

    tst = obj['runs'].show(uid)
    click.echo("{}".format(tst))

@run.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='Run UUID')
@click.pass_obj
@auth_required
def run_delete(obj, uid):

    tst = obj['runs'].delete(uid)
    click.echo("{}".format(tst))


### Reporter Commands ###

@cli.group()
@click.pass_obj
def reporter(obj):

    # Setup Client Class
    obj['reporters'] = api_client.Reporters(obj['connection'])

@reporter.command(name='create')
@click.option('--mod', default=None, prompt=True, help='Reporter Module')
@click.option('--mod_opt', 'mod_opts', nargs=2, multiple=True, help='Key:Value Option')
@click.pass_obj
@auth_required
def reporter_create(obj, mod, mod_opts):

    mod_kwargs = dict(list(mod_opts))
    rpt_list = obj['reporters'].create(mod, **mod_kwargs)
    click.echo("{}".format(rpt_list))

@reporter.command(name='update')
@click.option('--uid', prompt=True, type=click.UUID, help='Reporter UUID')
@click.option('--mod_opt', 'mod_opts', nargs=2, multiple=True, help='Key:Value Option')
@click.pass_obj
@auth_required
def reporter_update(obj, uid, mod_opts):

    mod_kwargs = dict(list(mod_opts))
    rpt = obj['reporters'].update(uid, **mod_kwargs)
    click.echo("{}".format(rpt))

@reporter.command(name='list')
@click.option('--tst_uid', default=None, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def reporter_list(obj, tst_uid):

    rpt_list = obj['reporters'].list(tst_uid=tst_uid)
    click.echo("{}".format(rpt_list))

@reporter.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='Reporter UUID')
@click.pass_obj
@auth_required
def reporter_show(obj, uid):

    rpt = obj['reporters'].show(uid)
    click.echo("{}".format(rpt))

@reporter.command(name='delete')
@click.option('--uid', prompt=True, type=click.UUID, help='Reporter UUID')
@click.pass_obj
@auth_required
def reporter_delete(obj, uid):

    rpt = obj['reporters'].delete(uid)
    click.echo("{}".format(rpt))

### User Commands ###

@cli.group()
@click.pass_obj
def user(obj):

    # Setup Client Class
    obj['users'] = api_client.Users(obj['connection'])

@user.command(name='list')
@click.pass_obj
@auth_required
def user_list(obj):

    usr_list = obj['users'].list()
    click.echo("{}".format(usr_list))

@user.command(name='show')
@click.option('--uid', prompt=True, type=click.UUID, help='User UUID')
@click.pass_obj
@auth_required
def user_show(obj, uid):

    usr = obj['users'].show(uid)
    click.echo("{}".format(usr))

@user.command(name='uid_to_name')
@click.argument('uid', type=click.UUID)
@click.pass_obj
@auth_required
def user_uid_to_name(obj, uid):

    name = obj['users'].uid_to_name(uid)
    click.echo("{}".format(name))

@user.command(name='name_to_uid')
@click.argument('username', type=click.STRING)
@click.pass_obj
@auth_required
def user_uid_to_name(obj, username):

    uid = obj['users'].name_to_uid(username)
    click.echo("{}".format(uid))


### Util Commands ###

@cli.group()
@click.pass_obj
def util(obj):

    # Setup Client Class
    obj['files'] = api_client.AsyncFiles(obj['connection'])
    obj['assignments'] = api_client.AsyncAssignments(obj['connection'])
    obj['tests'] = api_client.AsyncTests(obj['connection'])
    obj['submissions'] = api_client.AsyncSubmissions(obj['connection'])
    obj['runs'] = api_client.AsyncRuns(obj['connection'])
    obj['users'] = api_client.AsyncUsers(obj['connection'])
    obj['reporters'] = api_client.AsyncReporters(obj['connection'])

@util.command(name='save-config')
@click.argument('name')
@click.option('--conf_path', default=_PATH_SERVER_CONF, prompt=False,
              type=click.Path(resolve_path=True),
              help="Config Path ('{}')".format(_PATH_SERVER_CONF))
@click.pass_obj
@auth_required
def util_save_config(obj, name, conf_path):

    conf_obj = configparser.ConfigParser()

    if os.path.isfile(conf_path):
        click.echo("Reading existing '{}'".format(conf_path))
        conf_obj.read(conf_path)

    conf_dict = {}
    conf_dict['url'] = obj['connection'].get_url()
    conf_dict['user'] = obj['connection'].get_user()
    conf_dict['token'] = obj['connection'].get_token()
    click.echo("New config for '{}': {}".format(name, conf_dict))
    conf_obj[name] = conf_dict

    conf_dir = os.path.dirname(conf_path)
    os.makedirs(conf_dir, exist_ok=True)
    with open(conf_path, 'w') as conf_file:
        click.echo("Writing new config to '{}'".format(conf_path))
        conf_obj.write(conf_file)

@util.command(name='show-token')
@click.pass_obj
@auth_required
def util_show_token(obj):

    click.echo("WARNING: 'util show-token' is deprecated. Use 'my token' instead.", err=True)
    click.echo("'{}'".format(obj['connection'].get_token()))

@util.command(name='replace-test-files')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--tst_uid', prompt=True, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def util_replace_test_files(obj, path, extract, tst_uid):

    click.echo("Listing old files...")
    old_fle_list = obj['files'].list(tst_uid=tst_uid)
    click.echo("Old files:\n{}".format(old_fle_list))

    if old_fle_list:
        click.echo("Removing old files...")
        rem_fle_list = obj['tests'].detach_files(tst_uid, old_fle_list)
        click.echo("Remaining files:\n{}".format(rem_fle_list))

        click.echo("Deleting old files...")
        for fle_uid in old_fle_list:
            click.echo("Deleting file '{}'...".format(fle_uid))
            try:
                fle = obj['files'].delete(fle_uid)
            except Exception as e:
                click.echo("Failed to delete file '{}': {}".format(fle_uid, str(e)))
    else:
        click.echo("No old files found")

    click.echo("Creating new files...")
    new_fle_list = obj['files'].create(path, extract=extract)
    click.echo("New files:\n{}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = rem_fle_list = obj['tests'].attach_files(tst_uid, new_fle_list)
    click.echo("Attached files:\n{}".format(tst_fle_list))

@util.command(name='duplicate-test')
@click.option('--tst_uid', prompt=True, type=click.UUID, help='Test UUID')
@click.pass_obj
@auth_required
def util_duplicate_test(obj, tst_uid):

    click.echo("Getting original test...")
    orig_tst_uid = tst_uid
    orig_tst_obj = obj['tests'].show(orig_tst_uid)
    click.echo("Found test:\n{}".format(orig_tst_obj))

    click.echo("Creating new test...")
    new_tst_list = obj['tests'].create(orig_tst_obj['assignment'], orig_tst_obj['name'],
                                       orig_tst_obj['maxscore'], tester=orig_tst_obj['tester'],
                                       builder=orig_tst_obj['builder'],
                                       path_script=(orig_tst_obj['path_script'] if
                                                    orig_tst_obj['path_script'] else None))
    new_tst_uid = new_tst_list[0]
    click.echo("Created test:\n{}".format(new_tst_uid))

    click.echo("Getting original files...")
    orig_fle_list = obj['files'].list(tst_uid=orig_tst_uid)
    click.echo("Found files:\n{}".format(orig_fle_list))

    click.echo("Attaching files to new test...")
    new_fle_list = obj['tests'].attach_files(new_tst_uid, orig_fle_list)
    click.echo("Attached files:\n{}".format(new_fle_list))

@util.command(name='setup-assignment')
@click.option('--asn_name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default=None, prompt=True, help='Assignment Environment')
@click.option('--tst_name', default=None, prompt=True, help='Test Name')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.option('--tester', default='script', help='Test Module')
@click.option('--path_script', default=None, help='Relative Path to Grading Script')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--activate', is_flag=True, help='Control whether or not to make assignment live')
@click.option('--rptmod', default=None, help='Reporter Module')
@click.option('--rptmod_opt', 'rptmod_opts', nargs=2, multiple=True, help='Key:Value Option')
@click.pass_obj
@auth_required
def util_setup_assignment(obj, asn_name, env, tst_name, maxscore, tester,
                          path_script, path, extract, activate,
                          rptmod, rptmod_opts):

    click.echo("Creating assignment...")
    asn_list = obj['assignments'].create(asn_name, env=env)
    click.echo("Created assignments:\n{}".format(asn_list))
    asn_uid = asn_list[0]

    click.echo("Creating test...")
    tst_list = obj['tests'].create(asn_uid, tst_name, maxscore,
                                   tester=tester, path_script=path_script)
    click.echo("Created tests:\n{}".format(tst_list))
    tst_uid = tst_list[0]

    click.echo("Creating files...")
    new_fle_list = obj['files'].create(path, extract)
    click.echo("Created files:\n{}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = obj['tests'].attach_files(tst_uid, new_fle_list)
    click.echo("Attached files:\n{}".format(tst_fle_list))

    if rptmod:

        rptmod_kwargs = dict(list(rptmod_opts))
        click.echo("Creating reporter...")
        new_rpt_list = obj['reporters'].create(rptmod, **rptmod_kwargs)
        click.echo("Created reporters:\n{}".format(new_rpt_list))

        click.echo("Attaching reporters...")
        tst_rpt_list = obj['tests'].attach_reporters(tst_uid, new_rpt_list)
        click.echo("Attached reporters:\n{}".format(tst_rpt_list))

    if activate:

        click.echo("Activating Assignment...")
        obj['assignments'].update(asn_uid, accpeting_runs=True, accepting_subs=True)
        click.echo("Assignment Activated")

@util.command(name='setup-assignment-test')
@click.option('--asn_uid', prompt=True, type=click.UUID, help='Assignment UUID')
@click.option('--tst_name', default=None, prompt=True, help='Test Name')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.option('--tester', default='script', help='Test Module')
@click.option('--path_script', default=None, help='Relative Path to Grading Script')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--rptmod', default=None, help='Reporter Module')
@click.option('--rptmod_opt', 'rptmod_opts', nargs=2, multiple=True, help='Key:Value Option')
@click.pass_obj
@auth_required
def util_setup_assignment_test(obj, asn_uid, tst_name, maxscore, tester,
                               path_script, path, extract,
                               rptmod, rptmod_opts):

    click.echo("Creating test...")
    tst_list = obj['tests'].create(asn_uid, tst_name, maxscore,
                                   tester=tester, path_script=path_script)
    click.echo("Created tests:\n{}".format(tst_list))
    tst_uid = tst_list[0]

    click.echo("Creating files...")
    new_fle_list = obj['files'].create(path, extract)
    click.echo("Created files:\n{}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = obj['tests'].attach_files(tst_uid, new_fle_list)
    click.echo("Attached files:\n{}".format(tst_fle_list))

    if rptmod:

        rptmod_kwargs = dict(list(rptmod_opts))
        click.echo("Creating reporter...")
        new_rpt_list = obj['reporters'].create(rptmod, **rptmod_kwargs)
        click.echo("Created reporters:\n{}".format(new_rpt_list))

        click.echo("Attaching reporters...")
        tst_rpt_list = obj['tests'].attach_reporters(tst_uid, new_rpt_list)
        click.echo("Attached reporters:\n{}".format(tst_rpt_list))

@util.command(name='download-submissions')
@click.argument('dest_dir',
                type=click.Path(exists=True, writable=True,
                                resolve_path=True, file_okay=False))
@click.option('-a', '--asn_uid', 'asn_list',
              multiple=True, type=click.UUID, help='Limit to Assignment UUID')
@click.option('-s', '--sub_uid', 'sub_list',
              multiple=True, type=click.UUID, help='Limit to Submission UUID')
@click.option('-u', '--usr_uid', 'usr_uid_list',
              multiple=True, type=click.UUID, help='Limit to User UUID')
@click.option('--usr_name', 'usr_name_list',
              multiple=True, type=click.STRING, help='Limit to User Name')
@click.option('--full_uuid', is_flag=True,
              help='Force use of full UUIDs in output')
@click.option('--full_name', is_flag=True,
              help='Display full names instead of usernames in output')
@click.option('--show_timing', 'timing', is_flag=True,
              help='Collect and show timing data')
@click.option('--overwrite', is_flag=True,
              help='Overwrite existing files (skipped by default)')
@click.pass_obj
@auth_required
def util_download_submissions(obj, dest_dir, asn_list, sub_list,
                              usr_uid_list, usr_name_list,
                              full_uuid, full_name, timing, overwrite):

    # Start Timing
    if timing:
        start = time.time()

    # Make Async Calls
    with obj['connection']:

        # Convert usernames to UIDs
        if usr_name_list:
            usr_uids, usr_uids_failed = async_obj_map(usr_name_list, obj['users'].async_name_to_uid,
                                                      label="Getting  User UIDs  ", timing=timing)
        usr_uid_list = list(usr_uid_list)
        usr_uid_list += [usr_uids[name] for name in usr_name_list]

        # Fetch Assignments
        tup = async_obj_fetch([None], obj_name="Assignments", timing=timing,
                              async_list=obj['assignments'].async_list_by_null,
                              async_show=obj['assignments'].async_show,
                              prefilter_list=asn_list)
        asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

        # Fetch Submissions
        tup = async_obj_fetch(asn_objs.keys(), obj_name="Submissions", timing=timing,
                              async_list=obj['submissions'].async_list_by_asn,
                              async_show=obj['submissions'].async_show,
                              prefilter_list=sub_list,
                              postfilter_func=postfilter_attr_owner,
                              postfilter_func_args=[usr_uid_list])
        sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

        # Fetch Files
        tup = async_obj_fetch(sub_objs.keys(), obj_name="Files      ", timing=timing,
                              async_list=obj['files'].async_list_by_sub,
                              async_show=obj['files'].async_show)
        fle_lsts, fle_set, fle_objs, fle_lsts_failed, fle_objs_failed = tup

        # Fetch Users
        usr_set = set()
        for sub in sub_objs.values():
            usr_set.add(uuid.UUID(sub["owner"]))
        usr_objs, usr_objs_failed = async_obj_map(usr_set, obj['users'].async_show,
                                                  label="Getting  Users      ", timing=timing)

        # Build File Lists
        paths_map = {}
        for suid, fle_list in fle_lsts.items():

            sub = sub_objs[suid]
            usid = uuid.UUID(sub_objs[suid]['owner'])
            usr = usr_objs[usid]
            auid = uuid.UUID(sub_objs[suid]['assignment'])
            asn = asn_objs[auid]

            if full_uuid:
                sub_str = "sub_{}".format(str(suid))
                usr_str = "usr_{}".format(str(usid))
                asn_str = "asn_{}".format(str(auid))
            else:
                date = time.localtime(float(sub["created_time"]))
                date_str = time.strftime("%y%m%d_%H%M%S", date)
                sub_str = "sub_{}_{:012x}".format(date_str, suid.node)
                if full_name:
                    full_nme = "".join("{}_{}".format(usr['last'], usr['first']).split())
                    usr_str = "usr_{}_{:012x}".format(full_nme, usid.node)
                else:
                    user_nme = "".join(usr['username'].split())
                    usr_str = "usr_{}".format(user_nme)
                asgn_nme = "".join(asn['name'].split())
                asn_str = "asn_{}_{:012x}".format(asgn_nme, auid.node)

            sub_path = os.path.join(dest_dir, asn_str, usr_str, sub_str)
            os.makedirs(sub_path, exist_ok=True)

            for fuid in fle_list:
                rel_path = fle_objs[fuid]["name"]
                rel_path = util_cli.clean_path(rel_path)
                rel_path = util_cli.secure_path(rel_path)
                fle_path = os.path.join(sub_path, rel_path)
                paths_map[fle_path] = fuid

        paths_set = set(paths_map.keys())

        # Async Download Files
        def async_fun(path, paths_map):
            fuid = paths_map[path]
            return obj['files'].async_direct_download(fuid, path, overwrite=overwrite)
        label="Downloading Files   "
        paths_out, paths_failed = async_obj_map(paths_set, async_fun,
                                                label=label, timing=timing,
                                                async_func_args=[paths_map])

    # Display Errors:
    for puid, err in asn_lsts_failed:
        click.echo("Failed to list Assignments: {}".format(str(err)))
    for auid, err in asn_objs_failed.items():
        click.echo("Failed to get Assignment '{}': {}".format(auid, str(err)))
    for auid, err in sub_lsts_failed.items():
        click.echo("Failed to list Subs for Asn '{}': {}".format(audi, str(err)))
    for suid, err in sub_objs_failed.items():
        click.echo("Failed to get Submission '{}': {}".format(suid, str(err)))
    for suid, err in fle_lsts_failed.items():
        click.echo("Failed to list Files for Sub '{}': {}".format(suid, str(err)))
    for fuid, err in fle_objs_failed.items():
        click.echo("Failed to get File '{}': {}".format(fuid, str(err)))
    for path, err in paths_failed.items():
        basename = sys.path.basename(path)
        click.echo("Failed to download '{}': {}".format(basename, str(err)))

    # Display Stats:
    click.echo("Downloaded: {:6d} files".format(len(paths_out)))
    click.echo("Failed:     {:6d} files".format(len(paths_failed)))
    if timing:
        end = time.time()
        dur = end - start
        dur_str = "Duration:    {}".format(util_cli.duration_to_str(dur))
        ops = len(paths_set)/dur
        ops_str = "Files/sec:   {:11.2f}".format(ops)
        click.echo(dur_str)
        click.echo(ops_str)

@util.command(name='show-results')
@click.option('-a', '--asn_uid', 'asn_list',
              multiple=True, type=click.UUID, help='Limit to Assignment UUID')
@click.option('-t', '--tst_uid', 'tst_list',
              multiple=True, type=click.UUID, help='Limit to Test UUID')
@click.option('-s', '--sub_uid', 'sub_list',
              multiple=True, type=click.UUID, help='Limit to Submission UUID')
@click.option('-r', '--run_uid', 'run_list',
              multiple=True, type=click.UUID, help='Limit to Run UUID')
@click.option('-u', '--usr_uid', 'usr_uid_list',
              multiple=True, type=click.UUID, help='Limit to User UUID')
@click.option('--usr_name', 'usr_name_list',
              multiple=True, type=click.STRING, help='Limit to User Name')
@click.option('--sort_by', default=None,
              type=click.Choice(['User', 'Assignment', 'Test', 'Submission',
                                 'Run', 'Date', 'Status', 'Score']),
              help='Coulumn to sort data by')
@click.option('--line_limit', default=None, help='Limit output to line length')
@click.option('--full_uuid', is_flag=True,
              help='Force use of full UUIDs in output')
@click.option('--full_name', is_flag=True,
              help='Display full names instead of usernames in output')
@click.option('--show_timing', 'timing', is_flag=True,
              help='Collect and show timing data')
@click.option('--no_usr', is_flag=True,
              help='Disable display of User column')
@click.option('--no_asn', is_flag=True,
              help='Disable display of Assignment column')
@click.option('--no_tst', is_flag=True,
              help='Disable display of Test column')
@click.option('--no_sub', is_flag=True,
              help='Disable display of Sub column')
@click.option('--no_date', is_flag=True,
              help='Disable display of Date column')
@click.option('--no_status', is_flag=True,
              help='Disbale display of Status column')
@click.option('--no_score', is_flag=True,
              help='Control whether to display Score Column')
@click.pass_obj
@auth_required
def util_show_results(obj, asn_list, tst_list, sub_list, run_list,
                      usr_uid_list, usr_name_list,
                      sort_by, line_limit, full_uuid, full_name, timing,
                      no_usr, no_asn, no_tst, no_sub,
                      no_date, no_status, no_score):

    # Table Objects
    headings = ["Run"]
    if not no_date:
        headings.append("Date")
    if not no_usr:
        headings.append("User")
    if not no_asn:
        headings.append("Assignment")
    if not no_tst:
        headings.append("Test")
    if not no_sub:
        headings.append("Submission")
    if not no_status:
        headings.append("Status")
    if not no_score:
        headings.append("Score")
    if sort_by is None:
        if not no_date:
            sort_by = "Date"
        else:
            sort_by = "Run"
    table = []

    # Make Async Calls
    with obj['connection']:

        # Convert usernames to UIDs
        if usr_name_list:
            usr_uids, usr_uids_failed = async_obj_map(usr_name_list, obj['users'].async_name_to_uid,
                                                      label="Getting  User UIDs  ", timing=timing)
        usr_uid_list = list(usr_uid_list)
        usr_uid_list += [usr_uids[name] for name in usr_name_list]

        # Fetch Assignments
        tup = async_obj_fetch([None], obj_name="Assignments", timing=timing,
                              async_list=obj['assignments'].async_list_by_null,
                              async_show=obj['assignments'].async_show,
                              prefilter_list=asn_list)
        asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

        # Fetch Tests
        tup = async_obj_fetch(asn_objs.keys(), obj_name="Tests      ", timing=timing,
                              async_list=obj['tests'].async_list_by_asn,
                              async_show=obj['tests'].async_show,
                              prefilter_list=tst_list)
        tst_lsts, tst_set, tst_objs, tst_lsts_failed, tst_objs_failed = tup

        # Fetch Submissions
        tup = async_obj_fetch(asn_objs.keys(), obj_name="Submissions", timing=timing,
                              async_list=obj['submissions'].async_list_by_asn,
                              async_show=obj['submissions'].async_show,
                              prefilter_list=sub_list,
                              postfilter_func=postfilter_attr_owner,
                              postfilter_func_args=[usr_uid_list])
        sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

        # Fetch Runs
        tup = async_obj_fetch(sub_objs.keys(), obj_name="Runs       ", timing=timing,
                              async_list=obj['runs'].async_list_by_sub,
                              async_show=obj['runs'].async_show,
                              prefilter_list=run_list,
                              postfilter_func=postfilter_attr_test,
                              postfilter_func_args=[tst_list])
        run_lsts, run_set, run_objs, run_lsts_failed, run_objs_failed = tup

        # Fetch Users
        usr_set = set()
        for run in run_objs.values():
            usr_set.add(uuid.UUID(run["owner"]))
        usr_objs, usr_objs_failed = async_obj_map(usr_set, obj['users'].async_show,
                                                  label="Getting  Users      ", timing=timing)

    # Build Table Rows
    for ruid, run in run_objs.items():

        # Get Objects
        usid = uuid.UUID(run["owner"])
        usr = usr_objs[usid]
        suid = uuid.UUID(run["submission"])
        sub = sub_objs[suid]
        tuid = uuid.UUID(run["test"])
        tst = tst_objs[tuid]
        auid = uuid.UUID(sub["assignment"])
        asn = asn_objs[auid]

        # Display Objects
        if full_uuid:
            usr_str = str(usid)
            asn_str = str(auid)
            tst_str = str(tuid)
            sub_str = str(suid)
            run_str = str(ruid)
        else:
            if full_name:
                usr_str = "{}, {}".format(usr["last"], usr["first"])
            else:
                usr_str = usr["username"]
            asn_str = asn["name"]
            tst_str = tst["name"]
            sub_str = "{:012X}".format(suid.node)
            run_str = "{:012X}".format(ruid.node)

        # Display Date
        date = time.localtime(float(run["created_time"]))
        date_str = time.strftime("%m/%d/%y %H:%M:%S", date)

        # Display Results
        stat_str = run["status"]
        score_str = run["score"]

        # Add row
        row = [run_str]
        if not no_date:
            row.append(date_str)
        if not no_usr:
            row.append(usr_str)
        if not no_asn:
            row.append(asn_str)
        if not no_tst:
            row.append(tst_str)
        if not no_sub:
            row.append(sub_str)
        if not no_status:
            row.append(stat_str)
        if not no_score:
            row.append(score_str)
        table.append(row)

    # Display Errors:
    for puid, err in asn_lsts_failed:
        click.echo("Failed to list Assignments: {}".format(str(err)))
    for auid, err in asn_objs_failed.items():
        click.echo("Failed to get Assignment '{}': {}".format(auid, str(err)))
    for auid, err in tst_lsts_failed.items():
        click.echo("Failed to list Tests for Asn '{}': {}".format(auid, str(err)))
    for tuid, err in tst_objs_failed.items():
        click.echo("Failed to get Test '{}': {}".format(tuid, str(err)))
    for auid, err in sub_lsts_failed.items():
        click.echo("Failed to list Subs for Asn '{}': {}".format(audi, str(err)))
    for suid, err in sub_objs_failed.items():
        click.echo("Failed to get Submission '{}': {}".format(suid, str(err)))
    for suid, err in run_lsts_failed.items():
        click.echo("Failed to list Runs for Sub '{}': {}".format(suid, str(err)))
    for ruid, err in run_objs_failed.items():
        click.echo("Failed to get Run '{}': {}".format(ruid, str(err)))

    # Display Table
    util_click.echo_table(table, headings=headings,
                          line_limit=line_limit, sort_by=sort_by)

@util.command(name='cleanup')
@click.option('--all', 'cleanup_all', is_flag=True,
              help='Delete All Objects')
@click.option('--show_timing', 'timing', is_flag=True,
              help='Collect and show timing data')
@click.option('--assignments', 'cleanup_asn', is_flag=True,
              help='Delete Assigments')
@click.option('-a', '--asn_uid', 'asn_list',
              multiple=True, type=click.UUID, help='Limit to Assignment UUID')
@click.option('--tests', 'cleanup_tst', is_flag=True,
              help='Delete Tests')
@click.option('-t', '--tst_uid', 'tst_list',
              multiple=True, type=click.UUID, help='Limit to Test UUID')
@click.option('--submissions', 'cleanup_sub', is_flag=True,
              help='Delete Submissions')
@click.option('-s', '--sub_uid', 'sub_list',
              multiple=True, type=click.UUID, help='Limit to Submission UUID')
@click.option('--runs', 'cleanup_run', is_flag=True,
              help='Delete Runs')
@click.option('-r', '--run_uid', 'run_list',
              multiple=True, type=click.UUID, help='Limit to Run UUID')
@click.option('--files', 'cleanup_fle', is_flag=True,
              help='Delete Files')
@click.option('-f', '--file_uid', 'fle_list',
              multiple=True, type=click.UUID, help='Limit to File UUID')
@click.pass_obj
@auth_required
def util_cleanup(obj, cleanup_all, timing,
                 cleanup_asn, asn_list, cleanup_tst, tst_list,
                 cleanup_sub, sub_list, cleanup_run, run_list,
                 cleanup_fle, fle_list):

    # Make Async Calls
    with obj['connection']:

        if cleanup_asn or cleanup_all:

            # Fetch Assignments
            tup = async_obj_fetch([None], obj_name="Assignments", timing=timing,
                                  async_list=obj['assignments'].async_list_by_null,
                                  async_show=obj['assignments'].async_show,
                                  prefilter_list=asn_list)
            asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

            # Delete Assignments
            asn_deleted, asn_failed = async_obj_map(asn_set, obj['assignments'].async_delete,
                                                    label="Deleting Assignments",
                                                    timing=timing)

        if cleanup_tst or cleanup_all:

            # Fetch Tests
            tup = async_obj_fetch([None], obj_name="Tests      ", timing=timing,
                                  async_list=obj['tests'].async_list_by_null,
                                  async_show=obj['tests'].async_show,
                                  prefilter_list=tst_list)
            tst_lsts, tst_set, tst_objs, tst_lsts_failed, tst_objs_failed = tup

            # Delete Tests
            tst_deleted, tst_failed = async_obj_map(tst_set, obj['tests'].async_delete,
                                                    label="Deleting Tests      ",
                                                    timing=timing)

        if cleanup_sub or cleanup_all:

            # Fetch Submissions
            tup = async_obj_fetch([None], obj_name="Submissions", timing=timing,
                                  async_list=obj['submissions'].async_list_by_null,
                                  async_show=obj['submissions'].async_show,
                                  prefilter_list=sub_list)
            sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

            # Delete Submissions
            sub_deleted, sub_failed = async_obj_map(sub_set, obj['submissions'].async_delete,
                                                    label="Deleting Submissions",
                                                    timing=timing)

        if cleanup_run or cleanup_all:

            # Fetch Runs
            tup = async_obj_fetch([None], obj_name="Runs       ", timing=timing,
                                  async_list=obj['runs'].async_list_by_null,
                                  async_show=obj['runs'].async_show,
                                  prefilter_list=run_list)
            run_lsts, run_set, run_objs, run_lsts_failed, run_objs_failed = tup

            # Delete Runs
            run_deleted, run_failed = async_obj_map(run_set, obj['runs'].async_delete,
                                                    label="Deleting Runs       ",
                                                    timing=timing)

        if cleanup_fle or cleanup_all:

            # Fetch Files
            tup = async_obj_fetch([None], obj_name="Files      ", timing=timing,
                                  async_list=obj['files'].async_list_by_null,
                                  async_show=obj['files'].async_show,
                                  prefilter_list=fle_list)
            fle_lsts, fle_set, fle_objs, fle_lsts_failed, fle_objs_failed = tup

            # Delete Files
            fle_deleted, fle_failed = async_obj_map(fle_set, obj['files'].async_delete,
                                                    label="Deleting Files      ",
                                                    timing=timing)

    # Display Errors:

    if cleanup_asn or cleanup_all:

        for nuid, err in asn_lsts_failed:
            click.echo("Failed to list Assignments: {}".format(str(err)))
        for auid, err in asn_objs_failed.items():
            click.echo("Failed to fetch Assignment '{}': {}".format(auid, str(err)))
        for auid, err in asn_failed.items():
            click.echo("Failed to delete Assignment '{}': {}".format(auid, str(err)))

    if cleanup_tst or cleanup_all:

        for nuid, err in tst_lsts_failed:
            click.echo("Failed to list Tests: {}".format(str(err)))
        for tuid, err in tst_objs_failed.items():
            click.echo("Failed to fetch Test '{}': {}".format(tuid, str(err)))
        for tuid, err in tst_failed.items():
            click.echo("Failed to delete Test '{}': {}".format(tuid, str(err)))

    if cleanup_sub or cleanup_all:

        for nuid, err in sub_lsts_failed:
            click.echo("Failed to list Submissions: {}".format(str(err)))
        for suid, err in sub_objs_failed.items():
            click.echo("Failed to fetch Submission '{}': {}".format(suid, str(err)))
        for suid, err in sub_failed.items():
            click.echo("Failed to delete Submission '{}': {}".format(suid, str(err)))

    if cleanup_run or cleanup_all:

        for nuid, err in run_lsts_failed:
            click.echo("Failed to list Runs: {}".format(str(err)))
        for ruid, err in run_objs_failed.items():
            click.echo("Failed to fetch Run '{}': {}".format(ruid, str(err)))
        for ruid, err in run_failed.items():
            click.echo("Failed to delete Run '{}': {}".format(ruid, str(err)))

    if cleanup_fle or cleanup_all:

        for nuid, err in fle_lsts_failed:
            click.echo("Failed to list Files: {}".format(str(err)))
        for fuid, err in fle_objs_failed.items():
            click.echo("Failed to fetch File '{}': {}".format(fuid, str(err)))
        for fuid, err in fle_failed.items():
            click.echo("Failed to delete File '{}': {}".format(fuid, str(err)))


### Main ###

if __name__ == '__main__':
    sys.exit(cli())
