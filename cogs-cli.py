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
_ASSIGNMENT = ""

_PATH_ASSIGNMENT = 'assignments'
_PATH_TEST = 'tests'

_MODE_ASSIGNMENT = 'assignment'
_MODE_TEST = 'test'

_ACTION_CREATE = 'create'

def _main(argv=None):

    argv = argv or sys.argv[1:]

    # Setup Argument Parsing
    parser = argparse.ArgumentParser(description='COGS CLI')
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_asn = subparsers.add_parser(_MODE_ASSIGNMENT, help='Assignment Help')
    parser_asn.set_defaults(mode=_MODE_ASSIGNMENT)
    parser_asn.add_argument('action', type=str,
                            help='Action')

    parser_tst = subparsers.add_parser(_MODE_TEST, help='Assignment Help')
    parser_tst.set_defaults(mode=_MODE_TEST)
    parser_tst.add_argument('assignment', type=str,
                            help='Assignment UUID')
    parser_tst.add_argument('action', type=str,
                            help='Action')

    # Parse Arguments
    args = parser.parse_args(argv)
    mode = args.mode
    action = args.action

    if mode == _MODE_ASSIGNMENT:
        if action == _ACTION_CREATE:
            d = {'name': "Assignment",
                 'contact': "Andy Sayler"}
            djson = json.dumps(d)
            r = requests.post("{:s}/{:s}/".format(_HOST, _PATH_ASSIGNMENT), data=djson)
            stat = r.status_code
            if (stat != 200):
                raise Exception("Create Assignment Request Returned Error: {:d}".format(stat))
            print("Created Assignment {:s}".format(r.json()['assignments'][0]))
    elif mode == _MODE_TEST:
        asn = args.assignment
        if action == _ACTION_CREATE:
            d = {'name': "Test",
                 'contact': "Andy Sayler",
                 'type': "script",
                 'maxscore': "10"}
            djson = json.dumps(d)
            path = "{:s}/{:s}/{:s}/{:s}/".format(_HOST, _PATH_ASSIGNMENT, asn, _PATH_TEST)
            r = requests.post(path, data=djson)
            stat = r.status_code
            if (stat != 200):
                raise Exception("Create Test Request Returned Error: {:d}".format(stat))
            print("Created Test {:s}".format(r.json()['tests'][0]))

if __name__ == "__main__":
    sys.exit(_main())
