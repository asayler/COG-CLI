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
import datetime

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
@click.option('--tst_uid', default=None, prompt=True, help='Test UUID')
@click.pass_obj
@auth_required
def util_duplicate_test(obj, tst_uid):

    click.echo("Getting original test...")
    orig_tst_uid = tst_uid
    orig_tst_obj = obj['tests'].show(orig_tst_uid)
    click.echo("Found test:\n{}".format(orig_tst_obj))

    click.echo("Creating new test...")
    new_tst_list = obj['tests'].create(orig_tst_obj['assignment'], orig_tst_obj['name'],
                                       orig_tst_obj['tester'], orig_tst_obj['maxscore'])
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

@util.command(name='download-submissions')
@click.argument('dest_dir',
                type=click.Path(exists=True, writable=True,
                                resolve_path=True, file_okay=False))
@click.option('-a', '--asn_uid', 'asn_list',
              default=None, multiple=True, help='Limit to Assignment UUID')
@click.option('-s', '--sub_uid', 'sub_list',
              default=None, multiple=True, help='Limit to Submission UUID')
@click.option('-u', '--usr_uid', 'usr_list',
              default=None, multiple=True, help='Limit to User UUID')
@click.option('--full_uuid', is_flag=True,
              help='Force use of full UUIDs in output')
@click.option('--show_timing', 'timing', is_flag=True,
              help='Collect and show timing data')
@click.option('--overwrite', is_flag=True,
              help='Overwrite existing files (skipped by default)')
@click.pass_obj
@auth_required
def util_download_submissions(obj, dest_dir, asn_list, sub_list, usr_list,
                              full_uuid, timing, overwrite):

    # Start Timing
    if timing:
        start = time.time()

    # Make Async Calls
    with obj['connection']:

        # Fetch Assignments
        tup = async_obj_fetch([None], obj_name="Assignments", timing=timing,
                              async_list=obj['assignments'].async_list_by_null,
                              async_show=obj['assignments'].async_show,
                              prefilter_list=asn_list)
        asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

        # Fetch Submissions
        tup = async_obj_fetch(asn_set, obj_name="Submissions", timing=timing,
                              async_list=obj['submissions'].async_list_by_asn,
                              async_show=obj['submissions'].async_show,
                              prefilter_list=sub_list,
                              postfilter_func=postfilter_attr_owner,
                              postfilter_func_args=[usr_list])
        sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

        # Fetch Files
        tup = async_obj_fetch(sub_set, obj_name="Files      ", timing=timing,
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
            return obj['files'].async_direct_download(fuid, path, overwrite=overwrite)
        label="Downloading Files  "
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
        dur_str = "Duration:    {}".format(duration_to_str(dur))
        ops = len(paths_set)/dur
        ops_str = "Files/sec:   {:11.2f}".format(ops)
        click.echo(dur_str)
        click.echo(ops_str)

@util.command(name='show-results')
@click.option('-a', '--asn_uid', 'asn_list',
              default=None, multiple=True, help='Limit to Assignment UUID')
@click.option('-t', '--tst_uid', 'tst_list',
              default=None, multiple=True, help='Limit to Test UUID')
@click.option('-s', '--sub_uid', 'sub_list',
              default=None, multiple=True, help='Limit to Submission UUID')
@click.option('-r', '--run_uid', 'run_list',
              default=None, multiple=True, help='Limit to Run UUID')
@click.option('-u', '--usr_uid', 'usr_list',
              default=None, multiple=True, help='Limit to User UUID')
@click.option('--sort_by', default=None,
              type=click.Choice(['User', 'Assignment', 'Test', 'Submission',
                                 'Run', 'Date', 'Status', 'Score']),
              help='Coulumn to sort data by')
@click.option('--line_limit', default=None, help='Limit output to line length')
@click.option('--full_uuid', is_flag=True,
              help='Force use of full UUIDs in output')
@click.option('--show_timing', 'timing', is_flag=True,
              help='Collect and show timing data')
@click.option('--no_date', is_flag=True,
              help='Control whether to display run date and time')
@click.option('--no_status', is_flag=True,
              help='Control whether to display run status')
@click.option('--no_score', is_flag=True,
              help='Control whether to display run score')
@click.pass_obj
@auth_required
def util_show_results(obj, asn_list, tst_list, sub_list, run_list, usr_list,
                      sort_by, line_limit, full_uuid, timing,
                      no_date, no_status, no_score):

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

    # Make Async Calls
    with obj['connection']:

        # Fetch Assignments
        tup = async_obj_fetch([None], obj_name="Assignments", timing=timing,
                              async_list=obj['assignments'].async_list_by_null,
                              async_show=obj['assignments'].async_show,
                              prefilter_list=asn_list)
        asn_lsts, asn_set, asn_objs, asn_lsts_failed, asn_objs_failed = tup

        # Fetch Tests
        tup = async_obj_fetch(asn_set, obj_name="Tests      ", timing=timing,
                              async_list=obj['tests'].async_list_by_asn,
                              async_show=obj['tests'].async_show,
                              prefilter_list=tst_list)
        tst_lsts, tst_set, tst_objs, tst_lsts_failed, tst_objs_failed = tup

        # Fetch Submissions
        tup = async_obj_fetch(asn_set, obj_name="Submissions", timing=timing,
                              async_list=obj['submissions'].async_list_by_asn,
                              async_show=obj['submissions'].async_show,
                              prefilter_list=sub_list,
                              postfilter_func=postfilter_attr_owner,
                              postfilter_func_args=[usr_list])
        sub_lsts, sub_set, sub_objs, sub_lsts_failed, sub_objs_failed = tup

        # Fetch Runs
        tup = async_obj_fetch(sub_set, obj_name="Runs       ", timing=timing,
                              async_list=obj['runs'].async_list_by_sub,
                              async_show=obj['runs'].async_show,
                              prefilter_list=run_list,
                              postfilter_func=postfilter_attr_test,
                              postfilter_func_args=[tst_list])
        run_lsts, run_set, run_objs, run_lsts_failed, run_objs_failed = tup

    # Build Table Rows
    for ruid, run in run_objs.items():

        # Get Objects
        ruid = uuid.UUID(ruid)
        usid = uuid.UUID(run["owner"])
        suid = uuid.UUID(run["submission"])
        sub = sub_objs[str(suid)]
        tuid = uuid.UUID(run["test"])
        tst = tst_objs[str(tuid)]
        auid = uuid.UUID(sub["assignment"])
        asn = asn_objs[str(auid)]

        # Display Objects
        if full_uuid:
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
    click_util.echo_table(table, headings=headings,
                          line_limit=line_limit, sort_by=sort_by)

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
        dur_str = "Dur: {}".format(duration_to_str(dur))
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
            raise TypeError("Requires either obj_client ot async_list")
    label = "Listing {}".format(obj_name if obj_name else "")
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

def postfilter_attr(attr, ouid, obj, attrs):

    if attrs:
        return obj[attr] in attrs
    else:
        return True

def postfilter_attr_owner(ouid, obj, owners):

    return postfilter_attr("owner", ouid, obj, owners)

def postfilter_attr_test(ouid, obj, tests):

    return postfilter_attr("test", ouid, obj, tests)

def lists_to_set(lists):

    sset = set([ouid for puid, ouids in lists.items() for ouid in ouids])
    return sset

def split_duration(dur):

    hours, rem = divmod(dur, 3600)
    minutes, rem = divmod(rem, 60)
    seconds = rem
    return hours, minutes, seconds

def duration_to_str(dur):

    hours, minutes, seconds = split_duration(dur)
    return "{:02.0f}:{:02.0f}:{:05.2f}".format(hours, minutes, seconds)

if __name__ == '__main__':
    sys.exit(cli())
