#!/usr/bin/env python3

import sys
import json
import functools
import os
import shelve
import time
import uuid
import threading
import concurrent.futures
import queue

import requests
import click

import client
import click_util
import util as cli_util

_EP_TOKENS = 'tokens'

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
    ctx.obj['connection'] = client.AsyncConnection(ctx.obj['url'])

### File Commands ###

@cli.group(name='file')
@click.pass_obj
def fle(obj):

    # Setup Client Class
    obj['files'] = client.Files(obj['connection'])

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
@click.option('--tst_uid', default=None,
              help='Only list files attached to a specific test')
@click.option('--sub_uid', default=None,
              help='Only list files attached to a specific submission')
@click.pass_obj
@auth_required
def fle_list(obj, tst_uid, sub_uid):

    fle_list = obj['files'].list(tst_uid=tst_uid, sub_uid=sub_uid)
    click.echo("{}".format(fle_list))

@fle.command(name='show')
@click.option('--uid', default=None, prompt=True, help='File UUID')
@click.pass_obj
@auth_required
def fle_show(obj, uid):

    fle = obj['files'].show(uid)
    click.echo("{}".format(fle))

@fle.command(name='delete')
@click.option('--uid', default=None, prompt=True, help='File UUID')
@click.pass_obj
@auth_required
def fle_delete(obj, uid):

    fle = obj['files'].delete(uid)
    click.echo("{}".format(fle))

@fle.command(name='download')
@click.option('--uid', default=None, prompt=True, help='File UUID')
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
    obj['assignments'] = client.Assignments(obj['connection'])

@assignment.command(name='create')
@click.option('--name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default=None, prompt=True, help='Assignment Environment')
@click.pass_obj
@auth_required
def assignment_create(obj, name, env):

    asn_list = obj['assignments'].create(name, env)
    click.echo("{}".format(asn_list))

@assignment.command(name='list')
@click.option('--submitable', is_flag=True, help='Limit to submitable assignments')
@click.option('--runable', is_flag=True, help='Limit to runable assignments')
@click.pass_obj
@auth_required
def assignment_list(obj, submitable, runable):

    asn_list = obj['assignments'].list(submitable=submitable, runable=runable)
    click.echo("{}".format(asn_list))

@assignment.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_show(obj, uid):

    asn = obj['assignments'].show(uid)
    click.echo("{}".format(asn))

@assignment.command(name='delete')
@click.option('--uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
@auth_required
def assignment_delete(obj, uid):

    asn = obj['assignments'].delete(uid)
    click.echo("{}".format(asn))

### Test Commands ###

@cli.group()
@click.pass_obj
def test(obj):

    # Setup Client Class
    obj['tests'] = client.Tests(obj['connection'])

@test.command(name='create')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.option('--name', default=None, prompt=True, help='Test Name')
@click.option('--tester', default=None, prompt=True, help='Test Module')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.pass_obj
@auth_required
def test_create(obj, asn_uid, name, tester, maxscore):

    tst_list = obj['tests'].create(asn_uid, name, tester, maxscore)
    click.echo("{}".format(tst_list))

@test.command(name='list')
@click.option('--asn_uid', default=None, help='Assignment UUID')
@click.pass_obj
@auth_required
def test_list(obj, asn_uid):

    tst_list = obj['tests'].list(asn_uid=asn_uid)
    click.echo("{}".format(tst_list))

@test.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
@auth_required
def test_show(obj, uid):

    tst = obj['tests'].show(uid)
    click.echo("{}".format(tst))

@test.command(name='delete')
@click.option('--uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
@auth_required
def test_delete(obj, uid):

    tst = obj['tests'].delete(uid)
    click.echo("{}".format(tst))

@test.command(name='attach_files')
@click.option('--uid', default=None, prompt=True, help='Test UUID')
@click.option('--fle_uid', default=None, multiple=True, help='File UUID')
@click.pass_obj
@auth_required
def test_attach_files(obj, uid, fle_uid):

    tst = obj['tests'].attach_files(uid, fle_uid)
    click.echo("{}".format(tst))

@test.command(name='detach_files')
@click.option('--uid', default=None, prompt=True, help='Test UUID')
@click.option('--fle_uid', default=None, multiple=True, help='File UUID')
@click.pass_obj
@auth_required
def test_detach_files(obj, uid, fle_uid):

    tst = obj['tests'].detach_files(uid, fle_uid)
    click.echo("{}".format(tst))

### Submission Commands ###

@cli.group()
@click.pass_obj
def submission(obj):

    # Setup Client Class
    obj['submissions'] = client.Submissions(obj['connection'])

@submission.command(name='create')
@click.option('--asn_uid', default=None, prompt=True, help='Assignment UUID')
@click.pass_obj
@auth_required
def submission_create(obj, asn_uid):

    tst_list = obj['submissions'].create(asn_uid)
    click.echo("{}".format(tst_list))

@submission.command(name='list')
@click.option('--asn_uid', default=None, help='Assignment UUID')
@click.pass_obj
@auth_required
def submission_list(obj, asn_uid):

    tst_list = obj['submissions'].list(asn_uid=asn_uid)
    click.echo("{}".format(tst_list))

@submission.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Submission UUID')
@click.pass_obj
@auth_required
def submission_show(obj, uid):

    tst = obj['submissions'].show(uid)
    click.echo("{}".format(tst))

@submission.command(name='delete')
@click.option('--uid', default=None, prompt=True, help='Submission UUID')
@click.pass_obj
@auth_required
def submission_delete(obj, uid):

    tst = obj['submissions'].delete(uid)
    click.echo("{}".format(tst))

@submission.command(name='attach_files')
@click.option('--uid', default=None, prompt=True, help='Submission UUID')
@click.option('--fle_uid', default=None, multiple=True, help='File UUID')
@click.pass_obj
@auth_required
def submission_attach_files(obj, uid, fle_uid):

    tst = obj['submissions'].attach_files(uid, fle_uid)
    click.echo("{}".format(tst))

@submission.command(name='detach_files')
@click.option('--uid', default=None, prompt=True, help='Submission UUID')
@click.option('--fle_uid', default=None, multiple=True, help='File UUID')
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
    obj['runs'] = client.Runs(obj['connection'])

@run.command(name='create')
@click.option('--sub_uid', default=None, prompt=True, help='Submission UUID')
@click.option('--tst_uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
@auth_required
def run_create(obj, sub_uid, tst_uid):

    tst_list = obj['runs'].create(sub_uid, tst_uid)
    click.echo("{}".format(tst_list))

@run.command(name='list')
@click.option('--sub_uid', default=None, help='Submission UUID')
@click.pass_obj
@auth_required
def run_list(obj, sub_uid):

    tst_list = obj['runs'].list(sub_uid=sub_uid)
    click.echo("{}".format(tst_list))

@run.command(name='show')
@click.option('--uid', default=None, prompt=True, help='Run UUID')
@click.pass_obj
@auth_required
def run_show(obj, uid):

    tst = obj['runs'].show(uid)
    click.echo("{}".format(tst))

@run.command(name='delete')
@click.option('--uid', default=None, prompt=True, help='Run UUID')
@click.pass_obj
@auth_required
def run_delete(obj, uid):

    tst = obj['runs'].delete(uid)
    click.echo("{}".format(tst))

### Util Commands ###

@cli.group()
@click.pass_obj
def util(obj):

    # Setup Client Class
    obj['files'] = client.AsyncFiles(obj['connection'])
    obj['assignments'] = client.AsyncAssignments(obj['connection'])
    obj['tests'] = client.AsyncTests(obj['connection'])
    obj['submissions'] = client.AsyncSubmissions(obj['connection'])
    obj['runs'] = client.AsyncRuns(obj['connection'])

@util.command(name='show-token')
@click.pass_obj
@auth_required
def util_show_token(obj):

    click.echo("'{}'".format(obj['connection'].get_token()))

@util.command(name='replace-test-files')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.option('--tst_uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
@auth_required
def util_replace_test_files(obj, path, extract, tst_uid):

    click.echo("Listing old files...")
    old_fle_list = obj['files'].list(tst_uid=tst_uid)
    click.echo("Old files:\n{}".format(old_fle_list))

    click.echo("Removing old files...")
    rem_fle_list = obj['tests'].detach_files(tst_uid, old_fle_list)
    click.echo("Remaining files:\n{}".format(rem_fle_list))

    click.echo("Deleting old files...")
    for fle_uid in old_fle_list:
        click.echo("Deleting file '{}'...".format(fle_uid))
        fle = obj['files'].delete(fle_uid)

    click.echo("Creating new files...")
    new_fle_list = obj['files'].create(path, extract=extract)
    click.echo("New files:\n{}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = rem_fle_list = obj['tests'].attach_files(tst_uid, new_fle_list)
    click.echo("Attached files:\n{}".format(tst_fle_list))

@util.command(name='setup-assignment')
@click.option('--asn_name', default=None, prompt=True, help='Assignment Name')
@click.option('--env', default=None, prompt=True, help='Assignment Environment')
@click.option('--tst_name', default=None, prompt=True, help='Test Name')
@click.option('--tester', default=None, prompt=True, help='Test Module')
@click.option('--maxscore', default=None, prompt=True, help='Max Score')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, readable=True, resolve_path=True),
              help='Source Path')
@click.option('--extract', is_flag=True, help='Control whether file is extracted')
@click.pass_obj
@auth_required
def util_setup_assignment(obj, asn_name, env, tst_name, tester,
                          maxscore, path, extract):

    click.echo("Creating assignment...")
    asn_list = obj['assignments'].create(asn_name, env)
    click.echo("Created assignments:\n{}".format(asn_list))
    asn_uid = asn_list[0]

    click.echo("Creating test...")
    tst_list = obj['tests'].create(asn_uid, tst_name, tester, maxscore)
    click.echo("Created tests:\n{}".format(tst_list))
    tst_uid = tst_list[0]

    click.echo("Creating files...")
    new_fle_list = obj['files'].create(path, extract)
    click.echo("Created files:\n{}".format(new_fle_list))

    click.echo("Attaching files...")
    tst_fle_list = obj['tests'].attach_files(tst_uid, new_fle_list)
    click.echo("Attached files:\n{}".format(tst_fle_list))


def download_submission(obj, suid, fle_list, asn_dir_path):

    # Fetch Submission
    try:
        sub = obj['submissions'].show(suid)
    except requests.exceptions.HTTPError as err:
        return (None, [(suid, err)])

    # Build Submission Path
    ouid = sub['owner']
    own_dir_name = "user_{}".format(ouid)
    own_dir_path = os.path.join(asn_dir_path, own_dir_name)
    sub_dir_name = "submission_{}".format(suid)
    sub_dir_path = os.path.join(own_dir_path, sub_dir_name)
    os.makedirs(sub_dir_path, exist_ok=True)

    # Iterate Files
    fle_failed = []
    fle_success = []
    for fuid in fle_list:

        try:
            obj['files'].download(fuid, sub_dir_path, orig_path=True, overwrite=False)
        except requests.exceptions.HTTPError as err:
            fle_failed.append((fuid, err))
            continue
        else:
            fle_success.append(fuid)

    return (fle_success, fle_failed)

@util.command(name='download-submissions')
@click.argument('dest_dir', metavar="DEST_DIR",
                type=click.Path(exists=True, writable=True,
                                resolve_path=True, file_okay=False))
@click.option('-a', '--asn_uid', 'asn_list',
              default=None, multiple=True, help='Limit to Asn UUID')
@click.option('-s', '--sub_uid', 'sub_list',
              default=None, multiple=True, help='Sub UUID')
@click.option('-u', '--usr_uid', 'usr_list',
              default=None, multiple=True, help='User UUID')
@click.option('--full_uuid', is_flag=True,
              help='Force use of full UUIDs in output')
@click.pass_obj
@auth_required
def util_download_submissions(obj, dest_dir, asn_list, sub_list, usr_list, full_uuid):

    # COG Objects
    fles_todo = {}

    # Make Async Calls
    with obj['connection']:

        # Fetch Assignments
        tup = async_obj_fetch([None], obj_name="Assignments",
                              async_list=obj['assignments'].async_list_by_null,
                              async_show=obj['assignments'].async_show,
                              prefilter_list=asn_list)
        asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

        # Fetch Submissions
        tup = async_obj_fetch(asn_set, obj_name="Submissions",
                              async_list=obj['submissions'].async_list_by_asn,
                              async_show=obj['submissions'].async_show,
                              prefilter_list=sub_list,
                              postfilter_func=postfilter_owner,
                              postfilter_func_args=[usr_list])
        sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

        # Fetch Files
        tup = async_obj_fetch(sub_set, obj_name="Files      ",
                              async_list=obj['files'].async_list_by_sub,
                              async_show=obj['files'].async_show)
        fle_lsts, fle_set, fle_objs, fle_lsts_failed, fle_objs_failed = tup

        # Build File Lists
        paths_map = {}
        for suid, fle_list in fle_lsts.items():

            suid = uuid.UUID(suid)
            usid = uuid.UUID(sub_objs[str(suid)]['owner'])
            auid = uuid.UUID(sub_objs[str(suid)]['assignment'])

            if full_uuid:
                sub_str = str(suid)
                usr_str = str(usid)
                asn_str = str(auid)
            else:
                sub_str = "sub_{}".format(str(suid))
                usr_str = "usr_{}".format(str(usid))
                asn_nme = "".join(asn_objs[str(auid)]['name'].split())
                asn_str = "asn_{}_{:012x}".format(asn_nme, auid.node)

            sub_path = os.path.join(dest_dir, asn_str, usr_str, sub_str)
            os.makedirs(sub_path, exist_ok=True)

            for fuid in fle_list:
                rel_path = fle_objs[fuid]["name"]
                rel_path = cli_util.clean_path(rel_path)
                rel_path = cli_util.secure_path(rel_path)
                fle_path = os.path.join(sub_path, rel_path)
                paths_map[fle_path] = fuid

        paths_set = set(paths_map.keys())

        # Async Download Files
        def async_fun(path, paths_map):
            fuid = paths_map[path]
            return obj['files'].async_direct_download(fuid, path)
        label="Downloading Files  "
        paths_out, paths_failed = async_obj_map(paths_set, async_fun, label=label,
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
    click.echo("Downloaded {} files".format(len(paths_out)))
    click.echo("Failed {} files".format(len(paths_failed)))

@util.command(name='show-results')
@click.option('--asn_uid', default=None, help='Asn UUID')
@click.option('--tst_uid', default=None, help='Test UUID')
@click.option('--sub_uid', default=None, help='Sub UUID')
@click.option('--usr_uid', default=None, help='User UUID')
@click.option('--line_limit', default=None, help='Limit output to line length')
@click.option('--show_uuid', is_flag=True,
              help='Control whether to display names or full UUIDs')
@click.option('--no_date', is_flag=True,
              help='Control whether to display run date and time')
@click.option('--no_status', is_flag=True,
              help='Control whether to display run status')
@click.option('--no_score', is_flag=True,
              help='Control whether to display run score')
@click.option('--sort_by', default=None,
              type=click.Choice(['User', 'Assignment', 'Test', 'Submission',
                                 'Run', 'Date', 'Status', 'Score']),
              help='Control whether to display run score')
@click.pass_obj
@auth_required
def util_show_results(obj, asn_uid, tst_uid, sub_uid, usr_uid, line_limit,
                      show_uuid, no_date, no_status, no_score, sort_by):

    # Table Objects
    headings = ["User", "Assignment", "Test", "Submission", "Run"]
    if not no_date:
        headings.append("Date")
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

    # COG Objects
    asn_list = set()
    asn_list_failed = {}
    asns = {}
    asns_failed = {}
    tst_list = set()
    tst_list_failed = {}
    tsts = {}
    tsts_failed = {}
    sub_list = set()
    sub_list_failed = {}
    subs = {}
    subs_failed = {}
    run_list = set()
    run_list_failed = {}
    runs = {}
    runs_failed = {}

    # Make Async Calls
    with obj['connection']:

        # Async Assignment List
        def async_fun(uid):
            return obj['assignments'].async_list()
        label="Listing Assignments"
        output, failed = async_obj_map([None], async_fun, label=label)
        asn_list.update(set([auid for puid, asns in output.items() for auid in asns]))
        asn_list_failed.update(failed)

        # Pre-Filter Assignment List
        if asn_uid:
            if asn_uid in asn_list:
                asn_list = set([asn_uid])
            else:
                raise Exception("Assignment '{}' not found".format(asn_uid))

        # Async Get Assignments
        async_fun = obj['assignments'].async_show
        label="Getting Assignments"
        output, failed = async_obj_map(asn_list, async_fun, label=label)
        asns.update(output)
        asns_failed.update(failed)

        # Async Get Test Lists
        def async_fun(uid):
            return obj['tests'].async_list(asn_uid=uid)
        label="Listing Tests      "
        output, failed = async_obj_map(asn_list, async_fun, label=label)
        tst_list.update(set([tuid for puid, tsts in output.items() for tuid in tsts]))
        tst_list_failed.update(failed)

        # Pre-Filter Test List
        if tst_uid:
            if tst_uid in tst_list:
                tst_list = set([tst_uid])
            else:
                raise Exception("Test '{}' not found".format(tst_uid))

        # Async Get Tests
        async_fun = obj['tests'].async_show
        label="Getting Tests      "
        output, failed = async_obj_map(tst_list, async_fun, label=label)
        tsts.update(output)
        tsts_failed.update(failed)

        # Async Get Submission Lists
        def async_fun(uid):
            return obj['submissions'].async_list(asn_uid=uid)
        label="Listing Submissions"
        output, failed = async_obj_map(asn_list, async_fun, label=label)
        sub_list.update(set([suid for puid, subs in output.items() for suid in subs]))
        sub_list_failed.update(failed)

        # Pre-Filter Submission List
        if sub_uid:
            if sub_uid in sub_list:
                sub_list = set([sub_uid])
            else:
                raise Exception("Submission '{}' not found".format(sub_uid))

        # Async Get Submissions
        async_fun = obj['submissions'].async_show
        label="Getting Submissions"
        output, failed = async_obj_map(sub_list, async_fun, label=label)
        subs.update(output)
        subs_failed.update(failed)

        # Post-Filter Submission Set
        if usr_uid:
            sub_list = set()
            for suid, sub in subs.items():
                if sub["owner"] == usr_uid:
                    sub_list.add(suid)
            if not sub_list:
                raise Exception("No submissions for user '{}' found".format(usr_uid))

        # Async Get Run Lists
        def async_fun(uid):
            return obj['runs'].async_list(sub_uid=uid)
        label="Listing Runs       "
        output, failed = async_obj_map(sub_list, async_fun, label=label)
        run_list.update(set([ruid for puid, runs in output.items() for ruid in runs]))
        run_list_failed.update(failed)

        # Async Get Runs
        async_fun = obj['runs'].async_show
        label="Getting Runs       "
        output, failed = async_obj_map(run_list, async_fun, label=label)
        runs.update(output)
        runs_failed.update(failed)

    # Filter Results
    runs_filtered = {}
    for ruid, run in runs.items():

        # Filter Users
        if usr_uid:
            if (usr_uid != run["owner"]):
                continue

        # Filter Submissions
        if sub_uid:
            if (sub_uid != run["submission"]):
                continue

        # Filter Tests
        if tst_uid:
            if (tst_uid != run["test"]):
                continue

        runs_filtered[ruid] = run

    # Build Table Rows
    for ruid, run in runs_filtered.items():

        # Get Objects
        ruid = uuid.UUID(ruid)
        usid = uuid.UUID(run["owner"])
        suid = uuid.UUID(run["submission"])
        sub = subs[str(suid)]
        tuid = uuid.UUID(run["test"])
        tst = tsts[str(tuid)]
        auid = uuid.UUID(sub["assignment"])
        asn = asns[str(auid)]

        # Display Objects
        if show_uuid:
            usr_str = str(usid)
            asn_str = str(auid)
            tst_str = str(tuid)
            sub_str = str(suid)
            run_str = str(ruid)
        else:
            usr_str = "{:012X}".format(usid.node)
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
        row = [usr_str, asn_str, tst_str, sub_str, run_str]
        if not no_date:
            row.append(date_str)
        if not no_status:
            row.append(stat_str)
        if not no_score:
            row.append(score_str)
        table.append(row)

    # Display Errors:
    for puid, err in asn_list_failed:
        click.echo("Failed to list Assignments: {}".format(str(err)))
    for auid, err in asns_failed.items():
        click.echo("Failed to get Assignment '{}': {}".format(auid, str(err)))
    for auid, err in tst_list_failed.items():
        click.echo("Failed to list Tests for Asn '{}': {}".format(auid, str(err)))
    for tuid, err in tsts_failed.items():
        click.echo("Failed to get Test '{}': {}".format(tuid, str(err)))
    for auid, err in sub_list_failed.items():
        click.echo("Failed to list Subs for Asn '{}': {}".format(audi, str(err)))
    for suid, err in subs_failed.items():
        click.echo("Failed to get Submission '{}': {}".format(suid, str(err)))
    for suid, err in run_list_failed.items():
        click.echo("Failed to list Runs for Sub '{}': {}".format(suid, str(err)))
    for ruid, err in runs_failed.items():
        click.echo("Failed to get Run '{}': {}".format(ruid, str(err)))

    # Display Table
    click_util.echo_table(table, headings=headings,
                          line_limit=line_limit, sort_by=sort_by)

def async_obj_map(obj_list, async_fun,
                  async_func_args=[], async_func_kwargs={},
                  label=None, sleep=0.1):

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
                    except requests.exceptions.HTTPError as err:
                        failed[key] = err
                    finally:
                        bar.update(1)
                else:
                    future[key] = f
            time.sleep(sleep)

    return output, failed

def async_obj_fetch(iter_parent, obj_name=None, obj_client=None,
                    async_list=None, async_show=None,
                    prefilter_list=None, prefilter_func=None,
                    prefilter_func_args=[], prefilter_func_kwargs={},
                    postfilter_list=None, postfilter_func=None,
                    postfilter_func_args=[], postfilter_func_kwargs={}):

    # Async List
    if async_list is None:
        if obj_client is not None:
            async_list = obj_client.async_list()
        else:
            raise TypeError("Requires either obj_client ot async_list")
    label = "Listing {}".format(obj_name if obj_name else "")
    lists, lists_failed = async_obj_map(iter_parent, async_list, label=label)
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
                msg = "Pre-filtered {} '{}' not found".format(obj_str, ouid)
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
            raise TypeError("Requires either obj_client ot async_show")
    label = "Getting {}".format(obj_name if obj_name else "")
    objs, objs_failed = async_obj_map(todo_set, async_show, label=label)

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

def postfilter_owner(ouid, obj, owners):
    if owners:
        return obj["owner"] in owners
    else:
        return True

def lists_to_set(lists):

    sset = set([ouid for puid, ouids in lists.items() for ouid in ouids])
    return sset

if __name__ == '__main__':
    sys.exit(cli())
