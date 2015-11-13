#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Andy Sayler
# Summer 2014
# Univerity of Colorado

import sys
import argparse
import json

import requests

_HOST = "http://127.0.0.1:5000"

_KEY_ASSIGNMENT = 'assignments'
_KEY_SUBMISSION = 'submissions'
_KEY_FILE = 'files'
_KEY_RUN = 'runs'

def _main(argv=None):

    argv = argv or sys.argv[1:]

    # Setup Argument Parsing
    parser = argparse.ArgumentParser(description='Submit Assignment')
    parser.add_argument('assignment', type=str,
                        help='Assignment UUID')
    parser.add_argument('submission', type=str,
                        help='Submission File Name')

    # Parse Arguments
    args = parser.parse_args(argv)
    asn = args.assignment
    sub_path = args.submission

    # Create Submission
    d = {'author': "Andy Sayler"}
    djson = json.dumps(d)
    base_path = "{:s}/{:s}/{:s}/{:s}/".format(_HOST, _KEY_ASSIGNMENT, asn, _KEY_SUBMISSION)
    r = requests.post(base_path, data=djson)
    stat = r.status_code
    if (stat != 200):
        raise Exception("Create Submission Request Returned Error: {:d}".format(stat))
    sub = r.json()[_KEY_SUBMISSION][0]
    print("Created Submission {:s}".format(sub))

    # Upload Submission
    files = {'file': open(sub_path, 'rb')}
    file_path = "{:s}{:s}/{:s}/".format(base_path, sub, _KEY_FILE)
    r = requests.post(file_path, files=files)
    stat = r.status_code
    if (stat != 200):
        raise Exception("Upload File Request Returned Error: {:d}".format(stat))
    fle = r.json()[_KEY_FILE][0]
    print("Uploaded File {:s} as {:s}".format(sub_path, fle))

    # Run Submission
    run_path = "{:s}{:s}/{:s}/".format(base_path, sub, _KEY_RUN)
    r = requests.post(run_path)
    stat = r.status_code
    if (stat != 200):
        raise Exception("Run Submission Request Returned Error: {:d}".format(stat))
    run = r.json()[_KEY_RUN][0]
    print("Completed Test Run {:s}".format(run))

    # Get Results
    res_path = "{:s}{:s}/".format(run_path, run)
    r = requests.get(res_path)
    stat = r.status_code
    if (stat != 200):
        raise Exception("Get Results Request Returned Error: {:d}".format(stat))
    res = r.json()[run]
    print("Status: {:s}".format(res['status']))
    print("Output:\n{:s}".format(res['output']))
    print("Score: {:s}".format(res['score']))


if __name__ == "__main__":
    sys.exit(_main())
