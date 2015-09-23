# Andy Sayler
# COG API Client
# v2 API
# September 2015

import sys
import json
import abc

import requests

_EP_TOKENS = 'tokens'
_KEY_TOKENS = 'token'
_EP_ASSIGNMENTS = 'assignments'
_KEY_ASSIGNMENTS = 'assignments'
_EP_TESTS = 'tests'
_KEY_TESTS = 'tests'
_EP_FILES = 'files'
_KEY_FILES = 'files'

def _debug_dump(r):

    print(
        'Request:\n{}\n{}\n{}\n\n{}\n{}'.format(
            '-----------START-----------',
            r.request.method + ' ' + r.request.url,
            '\n'.join('{}: {}'.format(k, v) for k, v in r.request.headers.items()),
            r.request.body,
            '------------END------------'
        )
    )
    print('Response:\n{}'.format(r.text))

class Connection(object):

    def __init__(self, url, username=None, password=None, token=None):

        # Set vars
        self._url = url
        self._auth = None

        # Authenticate (if able)
        if token:
            self.authenticate(token=token)
        elif username and password:
            self.authenticate(username=username, password=password)

    def authenticate(self, username=None, password=None, token=None):

        endpoint = "{:s}/{:s}/".format(self._url, _EP_TOKENS)

        if token:

            # Verify Token
            auth = requests.auth.HTTPBasicAuth(token, '')
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            token = r.json()[_KEY_TOKENS]

        else:

            # Check Username/Password
            if not username or not password:
                raise TypeError("username and password required")

            # Get Token
            auth = requests.auth.HTTPBasicAuth(username, password)
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            token = r.json()[_KEY_TOKENS]

        self._auth = requests.auth.HTTPBasicAuth(token, '')

    def is_authenticated(self):
        if self._auth:
            return True
        else:
            return False

    def get_url(self):
        return self._url

    def http_post(self, endpoint, json=None, files=None):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.post(url, auth=self._auth, json=json, files=files)
        res.raise_for_status()
        return res.json()

    def http_get(self, endpoint):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.get(url, auth=self._auth)
        res.raise_for_status()
        return res.json()

    def http_delete(self, endpoint):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.delete(url, auth=self._auth)
        res.raise_for_status()
        return res.json()

class COGObject(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, connection):
        """ Constructor"""

        self._conn = connection
        self._ep = None
        self._key = None

    @abc.abstractmethod
    def create(self, endpoint=None, json=None, files=None):

        if endpoint is None:
            endpoint = self._ep

        res = self._conn.http_post(endpoint, json=json, files=files)
        uuid_list = res[self._key]
        return uuid_list

    def list(self):
        res = self._conn.http_get(self._ep)
        uuid_list = res[self._key]
        return uuid_list

    def show(self, uid):
        ep = "{:s}/{:s}".format(self._ep, uid)
        res = self._conn.http_get(ep)
        obj = res[uid]
        return obj

    def delete(self, uid):
        ep = "{:s}/{:s}".format(self._ep, uid)
        res = self._conn.http_delete(ep)
        obj = res[uid]
        return obj

class Assignments(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super(Assignments, self).__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_ASSIGNMENTS
        self._key = _KEY_ASSIGNMENTS

    def create(self, name, env):

        # Setup Data
        data = {'name': name, 'env': env}

        # Call Parent
        return super(Assignments, self).create(json=data)

class Tests(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super(Tests, self).__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_TESTS
        self._key = _KEY_TESTS

    def create(self, asn_uid, name, tester, maxscore):

        # Setup Data
        data = {"name": name, "tester": tester, "maxscore": maxscore}

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(_EP_ASSIGNMENTS, asn_uid, _EP_TESTS)

        # Call Parent
        return super(Tests, self).create(endpoint=ep, json=data)

# def test_file_add(url, auth, tst_uid, fle_uids):

#     endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
#     d = {_KEY_FILES: fle_uids}
#     dj = json.dumps(d)
#     r = requests.put(endpoint, auth=auth, data=dj)
#     r.raise_for_status()
#     fle_list = r.json()[_KEY_FILES]
#     return fle_list

# def test_file_remove(url, auth, tst_uid, fle_uids):

#     endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
#     d = {_KEY_FILES: fle_uids}
#     dj = json.dumps(d)
#     r = requests.delete(endpoint, auth=auth, data=dj)
#     r.raise_for_status()
#     fle_list = r.json()[_KEY_FILES]
#     return fle_list

# def file_create(url, auth, path, extract=False):

#     if extract:
#         key = 'extract'
#     else:
#         key = 'file'

#     endpoint = "{:s}/{:s}/".format(url, _EP_FILES)
#     files = {key: path}
#     r = requests.post(endpoint, files=files, auth=auth)
#     r.raise_for_status()
#     fle_list = r.json()[_KEY_FILES]
#     return fle_list

# def file_list(url, auth, tst_uid=None):

#     if tst_uid:
#         endpoint = "{:s}/{:s}/{:s}/{:s}/".format(url, _EP_TESTS, tst_uid, _EP_FILES)
#     else:
#         endpoint = "{:s}/{:s}/".format(url, _EP_FILES)
#     r = requests.get(endpoint, auth=auth)
#     r.raise_for_status()
#     fle_list = r.json()[_KEY_FILES]
#     return fle_list

# def file_show(url, auth, uid):

#     endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_FILES, uid)
#     r = requests.get(endpoint, auth=auth)
#     r.raise_for_status()
#     fle = r.json()[uid]
#     return fle

# def file_delete(url, auth, uid):

#     endpoint = "{:s}/{:s}/{:s}/".format(url, _EP_FILES, uid)
#     r = requests.delete(endpoint, auth=auth)
#     r.raise_for_status()
#     fle = r.json()[uid]
#     return fle
