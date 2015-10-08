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
                obj['connection'].authenticate(username=obj['username'], password=obj['password'])

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
@click.option('--tst_uid', default=None, help='Only list files attached to a specific test')
@click.option('--sub_uid', default=None, help='Only list files attached to a specific submission')
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
@click.option('--orig_path', is_flag=True, help='Control whether original path is used')
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
    obj['files'] = client.Files(obj['connection'])
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
def util_setup_assignment(obj, asn_name, env, tst_name, tester, maxscore, path, extract):

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
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, writable=True, resolve_path=True, file_okay=False),
              help='Destination Directory')
@click.option('--asn_uid', default=None, help='Assignment UUID')
@click.option('--sub_uid', default=None, help='Submission UUID')
@click.pass_obj
@auth_required
def util_download_submissions(obj, path, asn_uid, sub_uid):

    # Locals
    subs_cnt = 0
    files_cnt = 0
    files_index = {}
    download_cnt = 0
    download_index = {}
    completed_cnt = 0
    failed_cnt = 0
    asn_failed = []
    sub_failed = []
    fle_failed = []

    # Build Lengths
    click.echo("Calculating submission count...")
    if asn_uid:
        if sub_uid:
            subs_cnt = 1
        else:
            subs_cnt = len(obj['submissions'].list(asn_uid=asn_uid))
    else:
        subs_cnt = len(obj['submissions'].list())

    # Generate files_to_download
    label = "Processing  Submissions"
    with click.progressbar(label=label, length=subs_cnt) as bar:

        # Get Assignments
        if asn_uid:
            asn_list = [asn_uid]
        else:
            asn_list = obj['assignments'].list()

        # Iterate Assignments
        for auid in asn_list:

            # Get Submissions
            if sub_uid:
                sub_list = [sub_uid]
            else:
                sub_list = obj['submissions'].list(asn_uid=auid)
            files_index[auid] = {}

            # Iterate Submissions
            for suid in sub_list:

                # Get Files
                fle_list = obj['files'].list(sub_uid=suid)
                files_cnt += len(fle_list)
                files_index[auid][suid] = fle_list
                bar.update(1)

    # Setup Status Shelve
    prog_path = os.path.join(path, ".progress")
    with shelve.open(prog_path, writeback=True) as prog:

        # Filter Out Completed Items
        click.echo("Loading previous progress...")
        for auid, sub_files in files_index.items():
            sub_downloads = {}
            for suid, fle_list in sub_files.items():
                downloads_list = []
                for fuid in fle_list:
                    if fuid not in prog:
                        downloads_list.append(fuid)
                if downloads_list:
                    download_cnt += len(downloads_list)
                    sub_downloads[suid] = downloads_list
            if sub_downloads:
                download_index[auid] = sub_downloads

        # Process files_to_download
        label = "Downloading Submissions"
        with click.progressbar(label=label, length=download_cnt) as bar:

            # Iterate Assignments
            for auid, sub_files in download_index.items():

                # Fetch Assignment
                try:
                    asn = obj['assignments'].show(auid)
                except requests.exceptions.HTTPError as err:
                    asn_failed += (auid, err)
                    for suid, fle_list in sub_files.items():
                        bar.update(len(fle_list))
                    continue

                # Build Assignment Path
                asn_dir_name = "assignment_{}_{}".format(auid, "".join(asn['name'].split()))
                asn_dir_path = os.path.join(path, asn_dir_name)

                # Spin Threads
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exc:

                    # Iterate Submissions
                    futures = []
                    for suid, fle_list in sub_files.items():
                        f = exc.submit(download_submission, obj, suid, fle_list, asn_dir_path)
                        futures.append(f)

                    # Collect Results
                    while futures:
                        remaining = futures
                        futures = []
                        for f in remaining:
                            if f.done():
                                succ, fail = f.result()
                                if fail:
                                    if succ is None:
                                        sub_failed += fail
                                        failed_cnt += len(fle_list)
                                        bar.update(len(fle_list))
                                    else:
                                        fle_failed += fail
                                        failed_cnt += len(fail)
                                        bar.update(len(fail))
                                if succ:
                                    for fuid in succ:
                                        prog[fuid] = True
                                        completed_cnt += 1
                                        bar.update(1)
                            else:
                                futures.append(f)
                        time.sleep(0.1)

                    # Sync Progress
                    prog.sync()

    # Print Status
    click.echo("Downloaded {} files".format(completed_cnt))
    click.echo("Skipped {} files".format(files_cnt - download_cnt))
    click.echo("Failed {} files".format(failed_cnt))

    # Print Failures
    if asn_failed:
        click.echo("Failed Assignments:", err=True)
        for val in asn_failed:
            auid, err = val
            click.echo("{} - {}".format(auid, str(err)), err=True)
    if sub_failed:
        click.echo("Failed Submissions:", err=True)
        for val in sub_failed:
            suid, err = val
            click.echo("{} - {}".format(suid, str(err)), err=True)
    if fle_failed:
        click.echo("Failed Files:", err=True)
        for val in fle_failed:
            fuid, err = val
            click.echo("{} - {}".format(fuid, str(err)), err=True)

@util.command(name='show-results')
@click.option('--asn_uid', default=None, help='Asn UUID')
@click.option('--tst_uid', default=None, help='Test UUID')
@click.option('--sub_uid', default=None, help='Sub UUID')
@click.option('--usr_uid', default=None, help='User UUID')
@click.option('--line_limit', default=None, help='Limit output to line length')
@click.option('--show_uuid', is_flag=True, help='Control whether to display names or full UUIDs')
@click.option('--no_date', is_flag=True, help='Control whether to display run date and time')
@click.option('--no_status', is_flag=True, help='Control whether to display run status')
@click.option('--no_score', is_flag=True, help='Control whether to display run score')
@click.pass_obj
@auth_required
def util_show_results(obj, asn_uid, tst_uid, sub_uid, usr_uid, line_limit,
                      show_uuid, no_date, no_status, no_score):

    # Table Objects
    headings = ["User", "Assignment", "Test", "Submission", "Run"]
    if not no_date:
        headings.append("Date")
    if not no_status:
        headings.append("Status")
    if not no_score:
        headings.append("Score")
    table = []

    # COG Objects
    asn_list = set()
    asn_list_failed = []
    asns = {}
    asns_failed = {}
    tst_list = set()
    tst_list_failed = []
    tsts = {}
    tsts_failed = {}
    sub_list = set()
    sub_list_failed = []
    subs = {}
    subs_failed = {}
    run_list = set()
    run_list_failed = []
    runs = {}
    runs_failed = {}

    # Make Async Calls
    with obj['connection']:

        # Get Assignment List
        click.echo("Getting Assignment List...")
        try:
            asn_list.update(set(obj['assignments'].list()))
        except requests.exceptions.HTTPError as err:
            asn_list_failed.append(err)

        # Pre-Filter Assignment List
        if asn_uid:
            if asn_uid in asn_list:
                asn_list = set([asn_uid])
            else:
                raise Exception("Assignment '{}' not found".format(asn_uid))

        # Async Get Assignments
        asns, asns_failed = async_obj_show(asn_list, obj['assignments'].async_show,
                                           label="Getting Assignments")

        # Async Get Test Lists
        tst_lists_f = []
        for auid in asn_list:
            tst_lists_f.append(obj['tests'].async_list(asn_uid=auid))
        for tst_list_f in tst_lists_f:
            try:
                tst_list.update(set(tst_list_f.result()))
            except requests.exceptions.HTTPError as err:
                tst_list_failed.append(err)

        # Pre-Filter Test List
        if tst_uid:
            if tst_uid in tst_list:
                tst_list = set([tst_uid])
            else:
                raise Exception("Test '{}' not found".format(tst_uid))

        # Async Get Tests
        tsts, tsts_failed = async_obj_show(tst_list, obj['tests'].async_show,
                                           label="Getting Tests      ")

        # Async Get Submission Lists
        sub_lists_f = []
        for auid in asn_list:
            sub_lists_f.append(obj['submissions'].async_list(asn_uid=auid))
        for sub_list_f in sub_lists_f:
            try:
                sub_list.update(set(sub_list_f.result()))
            except requests.exceptions.HTTPError as err:
                sub_list_failed.append(err)

        # Pre-Filter Submission List
        if sub_uid:
            if sub_uid in sub_list:
                sub_list = set([sub_uid])
            else:
                raise Exception("Submission '{}' not found".format(sub_uid))

        # Async Get Submissions
        subs, subs_failed = async_obj_show(sub_list, obj['submissions'].async_show,
                                           label="Getting Submissions")

        # Async Get Run Lists
        run_lists_f = []
        for suid in sub_list:
            run_lists_f.append(obj['runs'].async_list(sub_uid=suid))
        for run_list_f in run_lists_f:
            try:
                run_list.update(set(run_list_f.result()))
            except requests.exceptions.HTTPError as err:
                run_list_failed.append(err)

        # Async Get Runs
        runs, runs_failed = async_obj_show(run_list, obj['runs'].async_show,
                                           label="Getting Runs       ")

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
    for err in asn_list_failed:
        click.echo("Faild to list Assignments: {}".format(str(err)))
    for auid, err in asns_failed.items():
        click.echo("Faild to get Assignment '{}': {}".format(auid, str(err)))
    for err in tst_list_failed:
        click.echo("Faild to list Tests: {}".format(str(err)))
    for tuid, err in tsts_failed.items():
        click.echo("Faild to get Test '{}': {}".format(tuid, str(err)))
    for err in sub_list_failed:
        click.echo("Faild to list Submissions: {}".format(str(err)))
    for suid, err in subs_failed.items():
        click.echo("Faild to get Submission '{}': {}".format(suid, str(err)))
    for err in run_list_failed:
        click.echo("Faild to list Runs: {}".format(str(err)))
    for ruid, err in runs_failed.items():
        click.echo("Faild to get Run '{}': {}".format(ruid, str(err)))

    # Display Table
    click_util.echo_table(table, headings=headings, line_limit=line_limit)

def async_obj_show(obj_list, async_get, label=None, sleep=0.1):

    output = {}
    failed = {}
    future = {}

    for uid in obj_list:
        future[uid] = async_get(uid)
    with click.progressbar(label=label, length=len(future)) as bar:
        while future:
            remain = future
            future = {}
            for uid, f in remain.items():
                if f.done():
                    try:
                        output[uid] = f.result()
                    except requests.exceptions.HTTPError as err:
                        failed[uid] = err
                    finally:
                        bar.update(1)
                else:
                    future[uid] = f
            time.sleep(sleep)

    return output, failed

if __name__ == '__main__':
    sys.exit(cli())
