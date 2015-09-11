# Andy Sayler
# COG API Client
# v2 API
# September 2015

import sys
import json

import requests

_EP_TOKENS = 'tokens'
_EP_ASSIGNMENTS = 'assignments'
_KEY_ASSIGNMENTS = 'assignments'
_EP_TESTS = 'tests'
_KEY_TESTS = 'tests'
_EP_FILES = 'files'
_KEY_FILES = 'files'

class Connection(object):

    def __init__(self, url, username=None, password=None, token=None):

        # Set vars
        self._url = url
        self._token = token
        self._auth = None

        # Authenticate (if able)
        if token:
            self.authenticate(token=token)
        elif username and password:
            self.authenticate(username=username, password=password)

    def authenticate(self, username=None, password=None, token=None):

        endpoint = "{:s}/{:s}/".format(obj['url'], _EP_TOKENS)

        if token:

            # Verify Token
            auth = requests.auth.HTTPBasicAuth(obj['token'], '')
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            self.token = r.json()['token']

        else:

            # Check Username/Password
            if not username or not password:
                raise TypeError("username and password required")

            # Get Token
            auth = requests.auth.HTTPBasicAuth(username, password)
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            self.token = r.json()['token']

        self.auth = requests.auth.HTTPBasicAuth(self.token, '')

    def is_authenticated(self):
        if self._auth:
            return True
        else:
            return False

    def get_url(self):
        return self._url

    def get_token(self):
        return self._token

def assignment_create(url, auth, asn_name, asn_env):

    endpoint = "{:s}/{:s}/".format(url, _EP_ASSIGNMENTS)
    d = {'name': asn_name, 'env': asn_env}
    dj = json.dumps(d)
    r = requests.post(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    asn_list = r.json()[_KEY_ASSIGNMENTS]
    return asn_list

def assignment_list(url, auth):

    endpoint = "{:s}/{:s}/".format(url, _EP_ASSIGNMENTS)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    asn_list = r.json()[_KEY_ASSIGNMENTS]
    return asn_list

def assignment_show(url, auth, uid):

    endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_ASSIGNMENTS, uid)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    asn = r.json()[uid]
    return asn

def assignment_test_create(url, auth, asn_uid, tst_name, tst_tester, tst_maxscore):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_ASSIGNMENTS, asn_uid, _EP_TESTS)
    d = {"name": tst_name, "tester": tst_tester, "maxscore": tst_maxscore}
    dj = json.dumps(d)
    r = requests.post(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    tst_list = r.json()[_KEY_TESTS]
    return tst_list

def test_file_add(url, auth, tst_uid, fle_uids):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
    d = {_KEY_FILES: fle_uids}
    dj = json.dumps(d)
    r = requests.put(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def test_file_remove(url, auth, tst_uid, fle_uids):

    endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
    d = {_KEY_FILES: fle_uids}
    dj = json.dumps(d)
    r = requests.delete(endpoint, auth=auth, data=dj)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def file_create(url, auth, path, extract=False):

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

def file_list(url, auth, tst_uid=None):

    if tst_uid:
        endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
    else:
        endpoint = "{:s}/{:s}/".format(url, _EP_FILES)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    fle_list = r.json()[_KEY_FILES]
    return fle_list

def file_show(url, auth, uid):

    endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_FILES, uid)
    r = requests.get(endpoint, auth=auth)
    r.raise_for_status()
    fle = r.json()[uid]
    return fle

def file_delete(url, auth, uid):

    endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_FILES, uid)
    r = requests.delete(endpoint, auth=auth)
    r.raise_for_status()
    fle = r.json()[uid]
    return fle
