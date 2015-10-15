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

@util.command(name='download-submissions2')
@click.option('--path', default=None, prompt=True,
              type=click.Path(exists=True, writable=True, resolve_path=True, file_okay=False),
              help='Destination Directory')
@click.option('--asn_uid', default=None, help='Asn UUID')
@click.option('--sub_uid', default=None, help='Sub UUID')
@click.option('--usr_uid', default=None, help='User UUID')
@click.pass_obj
@auth_required
def util_download_submissions2(obj, path, asn_uid, sub_uid, usr_uid):

    # Handle Input
    base_path = os.path.abspath(path)

    # COG Objects
    asn_set = set()
    asn_lists_failed = {}
    asns = {}
    asns_failed = {}
    sub_set = set()
    sub_lists_failed = {}
    subs = {}
    subs_failed = {}
    fle_set = set()
    fle_lists = {}
    fle_lists_failed = {}
    fles = {}
    fles_failed = {}
    fles_todo = {}
    paths_set = set()
    paths_out = {}
    paths_out_failed = {}

    # Make Async Calls
    with obj['connection']:

        # Async Assignment List
        def async_fun(uid):
            return obj['assignments'].async_list()
        label="Listing Assignments"
        output, failed = async_obj_map([None], async_fun, label=label)
        asn_set.update(set([auid for puid, auids in output.items() for auid in auids]))
        asn_lists_failed.update(failed)

        # Pre-Filter Assignment List
        if asn_uid:
            if asn_uid in asn_set:
                asn_set = set([asn_uid])
            else:
                raise Exception("Assignment '{}' not found".format(asn_uid))

        # Async Get Assignments
        async_fun = obj['assignments'].async_show
        label="Getting Assignments"
        output, failed = async_obj_map(asn_set, async_fun, label=label)
        asns.update(output)
        asns_failed.update(failed)

        # Async Get Submission Lists
        def async_fun(uid):
            return obj['submissions'].async_list(asn_uid=uid)
        label="Listing Submissions"
        output, failed = async_obj_map(asn_set, async_fun, label=label)
        sub_set.update(set([suid for auid, suids in output.items() for suid in suids]))
        sub_lists_failed.update(failed)

        # Pre-Filter Submission List
        if sub_uid:
            if sub_uid in sub_set:
                sub_set = set([sub_uid])
            else:
                raise Exception("Submission '{}' not found".format(sub_uid))

        # Async Get Submissions
        async_fun = obj['submissions'].async_show
        label="Getting Submissions"
        output, failed = async_obj_map(sub_set, async_fun, label=label)
        subs.update(output)
        subs_failed.update(failed)

        # Post-Filter Submission Set
        if usr_uid:
            sub_set = set()
            for suid, sub in subs.items():
                if sub["owner"] == usr_uid:
                    sub_set.add(suid)
            if not sub_set:
                raise Exception("No submissions for user '{}' found".format(usr_uid))

        # Async Get File Lists
        def async_fun(uid):
            return obj['files'].async_list(sub_uid=uid)
        label="Listing Files      "
        output, failed = async_obj_map(sub_set, async_fun, label=label)
        fle_set.update(set([fuid for suid, fuids in output.items() for fuid in fuids]))
        fle_lists.update(output)
        fle_lists_failed.update(failed)

        # Async Get Files
        async_fun = obj['files'].async_show
        label="Getting Files      "
        output, failed = async_obj_map(fle_set, async_fun, label=label)
        fles.update(output)
        fles_failed.update(failed)

        # Build File Lists
        for suid, fle_list in fle_lists.items():
            usid = subs[suid]['owner']
            auid = subs[suid]['assignment']
            asn_dir = "asn_{}_{}".format(auid, "".join(asns[auid]['name'].split()))
            usr_dir = "usr_{}".format(usid)
            sub_dir = "sub_{}".format(suid)
            sub_path = os.path.join(base_path, asn_dir, usr_dir, sub_dir)
            os.makedirs(sub_path, exist_ok=True)
            for fuid in fle_list:
                rel_path = fles[fuid]["name"]
                rel_path = cli_util.clean_path(rel_path)
                rel_path = cli_util.secure_path(rel_path)
                fle_path = os.path.join(sub_path, rel_path)
                fles_todo[fle_path] = fuid

        # Async Download Files
        def async_fun(fle_path, fles_todo):
            fuid = fles_todo[fle_path]
            return obj['files'].async_direct_download(fuid, fle_path)
        label="Downloading Files  "
        output, failed = async_obj_map(fles_todo, async_fun, fles_todo, label=label)
        paths_set.update(set([out_path for in_path, out_path in output.items()]))
        paths_out.update(output)
        paths_out_failed.update(failed)

    # Display Errors:
    for puid, err in asn_lists_failed:
        click.echo("Failed to list Assignments: {}".format(str(err)))
    for auid, err in asns_failed.items():
        click.echo("Failed to get Assignment '{}': {}".format(auid, str(err)))
    for auid, err in sub_lists_failed.items():
        click.echo("Failed to list Submissions for Assignment '{}': {}".format(audi, str(err)))
    for suid, err in subs_failed.items():
        click.echo("Failed to get Submission '{}': {}".format(suid, str(err)))
    for suid, err in fle_lists_failed.items():
        click.echo("Failed to list Files for Submission '{}': {}".format(suid, str(err)))
    for fuid, err in fles_failed.items():
        click.echo("Failed to get File '{}': {}".format(fuid, str(err)))
    for path, err in paths_out_failed.items():
        click.echo("Failed to download file '{}': {}".format(path, str(err)))

    # Display Stats:
    click.echo("Downloaded {} files".format(len(paths_out)))
    click.echo("Failed {} files".format(len(paths_out_failed)))


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
        click.echo("Failed to list Tests for Assignment '{}': {}".format(auid, str(err)))
    for tuid, err in tsts_failed.items():
        click.echo("Failed to get Test '{}': {}".format(tuid, str(err)))
    for auid, err in sub_list_failed.items():
        click.echo("Failed to list Submissions for Assignment '{}': {}".format(audi, str(err)))
    for suid, err in subs_failed.items():
        click.echo("Failed to get Submission '{}': {}".format(suid, str(err)))
    for suid, err in run_list_failed.items():
        click.echo("Failed to list Runs for Submission '{}': {}".format(suid, str(err)))
    for ruid, err in runs_failed.items():
        click.echo("Failed to get Run '{}': {}".format(ruid, str(err)))

    # Display Table
    click_util.echo_table(table, headings=headings, line_limit=line_limit, sort_by=sort_by)

def async_obj_map(obj_list, async_fun, *args, label=None, sleep=0.1, **kwargs):

    output = {}
    failed = {}
    future = {}

    for key in obj_list:
        future[key] = async_fun(key, *args, **kwargs)
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

def lists_to_set(lists):

    sset = set([ouid for puid, ouids in lists.items() for ouid in ouids])
    return sset

def async_obj_fetch(iter_parent, obj_client=None, async_list=None, async_show=None,
                    prefilter_list=None, obj_name=None):

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
    if filter_list:
        todo_set_orig = todo_set
        todo_set = set()
        for ouid in prefilter_list:
            if ouid in todo_set_orig:
                todo_set.add(ouid)
            else:
                msg = "Pre-filtered {} '{}'".format(obj_name if obj_name else "object", ouid)
                raise TypeError(msg)

    # Async Get
    if async_show is None:
        if obj_client is not None:
            async_show = obj_client.async_show()
        else:
            raise TypeError("Requires either obj_client ot async_show")
    label = "Getting {}".format(obj_name if obj_name else "")
    objs, objs_failed = async_obj_map(todo_set, async_show, label=label)

    # Return
    return lists, objs, lists_failed, objs_failed

if __name__ == '__main__':
    sys.exit(cli())
