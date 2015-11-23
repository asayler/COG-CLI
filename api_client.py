# Andy Sayler
# COG API Client
# v2 API
# September 2015

import sys
import json
import abc
import os
import os.path
import multiprocessing
import concurrent.futures
import functools

import requests

import util_cli

_EP_MY = 'my'
_EP_MY_TOKEN = 'token'
_KEY_MY_TOKEN = 'token'
_EP_MY_USERNAME = 'username'
_KEY_MY_USERNAME = 'username'
_EP_MY_USERUUID = 'useruuid'
_KEY_MY_USERUUID = 'useruuid'
_EP_FILES = 'files'
_EP_FILES_CONTENTS = 'contents'
_KEY_FILES = 'files'
_EP_ASSIGNMENTS = 'assignments'
_EP_ASSIGNMENTS_SUBMITABLE = 'submitable'
_EP_ASSIGNMENTS_RUNABLE = 'runable'
_KEY_ASSIGNMENTS = 'assignments'
_EP_TESTS = 'tests'
_KEY_TESTS = 'tests'
_EP_SUBMISSIONS = 'submissions'
_KEY_SUBMISSIONS = 'submissions'
_EP_RUNS = 'runs'
_KEY_RUNS = 'runs'
_EP_USERS = 'users'
_KEY_USERS = 'users'

_BLOCK_SIZE = 1024
_THREAD_MULTIPLIER = 5

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def authenticate(self, username=None, password=None, token=None):

        endpoint = "{:s}/{:s}/{:s}/".format(self._url, _EP_MY, _EP_MY_TOKEN)

        if token:

            # Verify Token
            auth = requests.auth.HTTPBasicAuth(token, '')
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            token = r.json()[_KEY_MY_TOKEN]

        else:

            # Check Username/Password
            if not username or not password:
                raise TypeError("username and password required")

            # Get Token
            auth = requests.auth.HTTPBasicAuth(username, password)
            r = requests.get(endpoint, auth=auth)
            r.raise_for_status()
            token = r.json()[_KEY_MY_TOKEN]

        self._auth = requests.auth.HTTPBasicAuth(token, '')

    def is_authenticated(self):
        if self._auth:
            return True
        else:
            return False

    def get_url(self):
        return self._url

    def get_user(self):
        return self.http_get("{}/{}".format(_EP_MY, _EP_MY_USERNAME))[_KEY_MY_USERNAME]

    def get_token(self):
        return self.http_get("{}/{}".format(_EP_MY, _EP_MY_TOKEN))[_KEY_MY_TOKEN]

    def http_post(self, endpoint, json=None, files=None):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.post(url, auth=self._auth, json=json, files=files)
        res.raise_for_status()
        return res.json()

    def http_put(self, endpoint, json=None):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.put(url, auth=self._auth, json=json)
        res.raise_for_status()
        return res.json()

    def http_get(self, endpoint=None, json=None):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.get(url, auth=self._auth, json=json)
        res.raise_for_status()
        return res.json()

    def http_delete(self, endpoint, json=None):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.delete(url, auth=self._auth, json=json)
        res.raise_for_status()
        return res.json()

    def http_download(self, endpoint, path):
        url = "{:s}/{:s}/".format(self._url, endpoint)
        res = requests.get(url, auth=self._auth)
        res.raise_for_status()
        with open(path, 'wb') as fd:
            for chunk in res.iter_content(chunk_size=_BLOCK_SIZE):
                    fd.write(chunk)
        return path

class AsyncConnection(Connection):

    def __init__(self, *args, threads=None, connection=None, **kwargs):

        if connection is None:
            # Call Parent
            super().__init__(*args, **kwargs)
        else:
            super().__init__(connection.get_url(), token=connection.get_token())

        # Handle Args
        if threads is None:
            self.threads = multiprocessing.cpu_count() * _THREAD_MULTIPLIER
        elif threads > 0:
            self.threads = threads
        else:
            raise TypeError("Threads must be greater than 0")

        # Setup Vars
        self._executor = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def open(self):
        mw = self.threads
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=mw)

    def close(self, wait=True):
        self._executor.shutdown(wait=wait)
        self._executor = None

    def is_open(self):
        if self._executor:
            return True
        else:
            return False

    def submit(self, fun, *args, **kwargs):

        # Open if closed
        opened = False
        if not self.is_open():
            self.open()
            opened = True

        # Call Function
        ret = self._executor.submit(fun, *args, **kwargs)

        # Close if opened
        if opened:
            self.close()

        return ret

    def async_http_post(self, *args, **kwargs):
        return self.submit(self.http_post, *args, **kwargs)

    def async_http_put(self, *args, **kwargs):
        return self.submit(self.http_put, *args, **kwargs)

    def async_http_get(self, *args, **kwargs):
        return self.submit(self.http_get, *args, **kwargs)

    def async_http_delete(self, *args, **kwargs):
        return self.submit(self.http_delete, *args, **kwargs)

    def async_http_download(self, *args, **kwargs):
        return self.submit(self.http_download, *args, **kwargs)

class MyInfo(object):

    def __init__(self, connection):
        """ Constructor"""

        # Save Params
        self._conn = connection
        self._ep = _EP_MY

    def token(self):
        return self._conn.http_get("{}/{}".format(_EP_MY, _EP_MY_TOKEN))[_KEY_MY_TOKEN]

    def username(self):
        return self._conn.http_get("{}/{}".format(_EP_MY, _EP_MY_USERNAME))[_KEY_MY_USERNAME]

    def useruuid(self):
        return self._conn.http_get("{}/{}".format(_EP_MY, _EP_MY_USERUUID))[_KEY_MY_USERUUID]

class AsyncMyInfo(MyInfo):

    def __init__(self, async_connection):
        """ Constructor"""

        # Check Type
        if type(async_connection) is not AsyncConnection:
            raise TypeError("Connection must be AsyncConnection")

        # Call Parent
        super().__init__(async_connection)

    def async_token(self, *args, **kwargs):
        return self._conn.submit(self.token, *args, **kwargs)

    def async_username(self, *args, **kwargs):
        return self._conn.submit(self.username, *args, **kwargs)

    def async_useruuid(self, *args, **kwargs):
        return self._conn.submit(self.useruuid, *args, **kwargs)

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

    def update(self, uid, json):
        ep = "{:s}/{:s}".format(self._ep, uid)
        res = self._conn.http_put(ep, json=json)
        obj = res[uid]
        return obj

    def list(self, endpoint=None):

        if endpoint is None:
            endpoint = self._ep

        res = self._conn.http_get(endpoint)
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

class AsyncCOGObject(COGObject):

    @abc.abstractmethod
    def __init__(self, async_connection):
        """ Constructor"""

        # Check Type
        if type(async_connection) is not AsyncConnection:
            raise TypeError("Connection must be AsyncConnection")

        # Call Parent
        super().__init__(async_connection)

    def async_create(self, *args, **kwargs):
        return self._conn.submit(self.create, *args, **kwargs)

    def async_list(self, *args, **kwargs):
        return self._conn.submit(self.list, *args, **kwargs)

    def async_update(self, *args, **kwargs):
        return self._conn.submit(self.update, *args, **kwargs)

    def async_show(self, *args, **kwargs):
        return self._conn.submit(self.show, *args, **kwargs)

    def async_delete(self, *args, **kwargs):
        return self._conn.submit(self.delete, *args, **kwargs)

class COGFileAttachedObject(COGObject):

    def attach_files(self, uid, fle_uids):

        # Check Args
        if not fle_uids:
            raise TypeError("fle_uids must not be empty")

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(self._ep, uid, _EP_FILES)

        # Setup Data
        data = {_KEY_FILES: fle_uids}

        # HTTP Call
        res = self._conn.http_put(endpoint=ep, json=data)
        lst = res[_KEY_FILES]
        return lst

    def detach_files(self, uid, fle_uids):

        # Check Args
        if not fle_uids:
            raise TypeError("fle_uids must not be empty")

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(self._ep, uid, _EP_FILES)

        # Setup Data
        data = {_KEY_FILES: fle_uids}

        # HTTP Call
        res = self._conn.http_delete(endpoint=ep, json=data)
        lst = res[_KEY_FILES]
        return lst

class AsyncCOGFileAttachedObject(COGFileAttachedObject, AsyncCOGObject):

    def async_attach_files(self, *args, **kwargs):
        return self._conn.submit(self.attach_file, *args, **kwargs)

    def async_detach_files(self, *args, **kwargs):
        return self._conn.submit(self.detach_file, *args, **kwargs)

class Files(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_FILES
        self._key = _KEY_FILES

    def create(self, path, extract=False):

        # Process Args
        if extract:
            key = 'extract'
        else:
            key = 'file'

        # Setup Files
        files = {key: open(path, 'rb')}

        # Call Parent
        return super().create(files=files)

    def list(self, tst_uid=None, sub_uid=None):

        # Setup Endpoint
        if tst_uid:
            ep = "{:s}/{:s}/{:s}".format(_EP_TESTS, tst_uid, _EP_FILES)
        elif sub_uid:
            ep = "{:s}/{:s}/{:s}".format(_EP_SUBMISSIONS, sub_uid, _EP_FILES)
        else:
            ep = self._ep

        # Call Parent
        return super().list(endpoint=ep)

    def list_by_tst(self, tst_uid):
        return self.list(tst_uid=tst_uid)

    def list_by_sub(self, sub_uid):
        return self.list(sub_uid=sub_uid)

    def download(self, uid, path, orig_path=False, overwrite=False):

        # Clean Input
        path = os.path.abspath(path)

        # Process Directory Path
        if os.path.isdir(path):
            fle_obj = self.show(uid)
            fle_path = fle_obj["name"]
            fle_path = util_cli.clean_path(fle_path)
            fle_path = util_cli.secure_path(fle_path)
            fle_name = os.path.basename(fle_path)
            if orig_path:
                path = os.path.join(path, fle_path)
            else:
                path = os.path.join(path, fle_name)

        if overwrite or not os.path.exists(path):

            # Create Directory
            dir_path = os.path.dirname(path)
            os.makedirs(dir_path, exist_ok=True)

            # Download File
            ep = "{:s}/{:s}/{:s}/".format(self._ep, uid, _EP_FILES_CONTENTS)
            path = self._conn.http_download(ep, path)

        return path

    def direct_download(self, uid, path, overwrite=False):

        # Clean Input
        path = os.path.abspath(path)

        if overwrite or not os.path.exists(path):

            # Download File
            ep = "{:s}/{:s}/{:s}/".format(self._ep, uid, _EP_FILES_CONTENTS)
            path = self._conn.http_download(ep, path)

        return path

class AsyncFiles(Files, AsyncCOGObject):

    def async_list_by_tst(self, *args, **kwargs):
        return self._conn.submit(self.list_by_tst, *args, **kwargs)

    def async_list_by_sub(self, *args, **kwargs):
        return self._conn.submit(self.list_by_sub, *args, **kwargs)

    def async_download(self, *args, **kwargs):
        return self._conn.submit(self.download, *args, **kwargs)

    def async_direct_download(self, *args, **kwargs):
        return self._conn.submit(self.direct_download, *args, **kwargs)

class Assignments(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_ASSIGNMENTS
        self._key = _KEY_ASSIGNMENTS

    def create(self, name, env='local',
               duedate=None, respect_duedate=False,
               accepting_runs=False, accepting_subs=False):

        # Setup Data
        data = {'name': name, 'env': env}
        data['duedate'] = str(duedate) if duedate else ''
        data['respect_duedate'] = '1' if respect_duedate else '0'
        data['accepting_runs'] = '1' if accepting_runs else '0'
        data['accepting_submissions'] = '1' if accepting_subs else '0'

        # Call Parent
        return super().create(json=data)

    def update(self, uid, name=None, env=None,
               duedate=None, respect_duedate=None,
               accepting_runs=None, accepting_subs=None):

        # Setup Data
        data = {}
        if name is not None:
            data['name'] = name
        if env is not None:
            data['env'] = env
        if duedate is not None:
            data['duedate'] = str(duedate) if duedate else ''
        if respect_duedate is not None:
            data['respect_duedate'] = '1' if respect_duedate else '0'
        if accepting_runs is not None:
            data['accepting_runs'] = '1' if accepting_runs else '0'
        if accepting_subs is not None:
            data['accepting_submissions'] = '1' if accepting_subs else '0'

        # Call Parent
        return super().update(uid, json=data)

    def list(self, submitable=False, runable=False):

        # Limted Cases
        if submitable or runable:

            submittable_set = set([])
            if submitable:
                ep = "{:s}/{:s}".format(_EP_ASSIGNMENTS, _EP_ASSIGNMENTS_SUBMITABLE)
                submittable_set = set(super().list(endpoint=ep))

            runable_set = set([])
            if runable:
                ep = "{:s}/{:s}".format(_EP_ASSIGNMENTS, _EP_ASSIGNMENTS_SUBMITABLE)
                runable_set = set(super().list(endpoint=ep))

            # Combine
            if submitable and runable:
                asn_list = list(submittable_set.intersection(runable_set))
            else:
                asn_list = list(submittable_set.union(runable_set))

        # Open Case
        else:

            ep = self._ep
            asn_list = super().list(endpoint=ep)

        # Call Parent
        return asn_list

    def list_by_null(self, null_uid):
        return self.list()

class AsyncAssignments(Assignments, AsyncCOGObject):

    def async_list_by_null(self, *args, **kwargs):
        return self._conn.submit(self.list_by_null, *args, **kwargs)

class Tests(COGFileAttachedObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_TESTS
        self._key = _KEY_TESTS

    def create(self, asn_uid, name, maxscore, tester='script'):

        # Setup Data
        data = {"name": name, "tester": tester, "maxscore": maxscore}

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(_EP_ASSIGNMENTS, asn_uid, _EP_TESTS)

        # Call Parent
        return super().create(endpoint=ep, json=data)

    def list(self, asn_uid=None):

        # Setup Endpoint
        if asn_uid:
            ep = "{:s}/{:s}/{:s}".format(_EP_ASSIGNMENTS, asn_uid, _EP_TESTS)
        else:
            ep = self._ep

        # Call Parent
        return super().list(endpoint=ep)

    def list_by_asn(self, asn_uid):
        return self.list(asn_uid=asn_uid)

class AsyncTests(Tests, AsyncCOGFileAttachedObject):

    def async_list_by_asn(self, *args, **kwargs):
        return self._conn.submit(self.list_by_asn, *args, **kwargs)

class Submissions(COGFileAttachedObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_SUBMISSIONS
        self._key = _KEY_SUBMISSIONS

    def create(self, asn_uid):

        # Setup Data
        data = {}

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(_EP_ASSIGNMENTS, asn_uid, _EP_SUBMISSIONS)

        # Call Parent
        return super().create(endpoint=ep, json=data)

    def list(self, asn_uid=None):

        # Setup Endpoint
        if asn_uid:
            ep = "{:s}/{:s}/{:s}".format(_EP_ASSIGNMENTS, asn_uid, _EP_SUBMISSIONS)
        else:
            ep = self._ep

        # Call Parent
        return super().list(endpoint=ep)

    def list_by_asn(self, asn_uid):
        return self.list(asn_uid=asn_uid)

class AsyncSubmissions(Submissions, AsyncCOGFileAttachedObject):

    def async_list_by_asn(self, *args, **kwargs):
        return self._conn.submit(self.list_by_asn, *args, **kwargs)

class Runs(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_RUNS
        self._key = _KEY_RUNS

    def create(self, sub_uid, tst_uid):

        # Setup Data
        data = {"test": tst_uid}

        # Setup Endpoint
        ep = "{:s}/{:s}/{:s}".format(_EP_SUBMISSIONS, sub_uid, _EP_RUNS)

        # Call Parent
        return super().create(endpoint=ep, json=data)

    def list(self, sub_uid=None):

        # Setup Endpoint
        if sub_uid:
            ep = "{:s}/{:s}/{:s}".format(_EP_SUBMISSIONS, sub_uid, _EP_RUNS)
        else:
            ep = self._ep

        # Call Parent
        return super().list(endpoint=ep)

    def list_by_sub(self, sub_uid):
        return self.list(sub_uid=sub_uid)

class AsyncRuns(Runs, AsyncCOGObject):

    def async_list_by_sub(self, *args, **kwargs):
        return self._conn.submit(self.list_by_sub, *args, **kwargs)

class Users(COGObject):

    def __init__(self, connection):
        """ Constructor"""

        # Call Parent
        super().__init__(connection)

        #Set Base Key and Endpoint
        self._ep = _EP_USERS
        self._key = _KEY_USERS

    def create(self, *args, **kwargs):
        raise NotImplementedError()

    def delete(self, *args, **kwargs):
        raise NotImplementedError()

class AsyncUsers(Users, AsyncCOGObject):

    def async_create(self, *args, **kwargs):
        raise NotImplementedError()

    def async_delete(self, *args, **kwargs):
        raise NotImplementedError()
