# Andy Sayler
# Fall 2015
# University of Colorado
# From https://github.com/asayler/COG/blob/master/cogs/util.py

import collections
import string
import os.path

VALID_FILENAME_CHARS = "+-_.() {}{}".format(string.ascii_letters, string.digits)

def split_path(path):
    """ Split a path into a list of components """

    components = collections.deque([])

    head = path
    while(head):
        head, tail = os.path.split(head)
        components.appendleft(tail)
        if head == os.path.sep:
            components.appendleft(head)
            head = ""

    return list(components)

def join_path(components):
    """ Join a list of components into a path """

    return os.path.join(*components)

def clean_filename(filename):
    """ Remove illegal filename characters from filename """

    filename = ''.join(c for c in filename if c in VALID_FILENAME_CHARS)
    filename = filename.strip()
    return filename

def clean_path(path):
    """ Remove illegal filename characters from path """

    components_in = split_path(path)
    components_out = []

    for name_in in components_in:
        if (name_in != os.path.sep):
            name_out = clean_filename(name_in)
        else:
            name_out = name_in
        components_out.append(name_out)

    return join_path(components_out)

def secure_path(path):
    """ Normlize and strip out leading recursive path operators """

    root = os.path.sep

    root_path = os.path.join(root, path)
    norm_path = os.path.normpath(root_path)
    comm_path = os.path.commonprefix([root, norm_path])
    rel_path = os.path.relpath(norm_path, start=comm_path)

    if os.path.isabs(path):
        sec_path = os.path.join(os.path.sep, rel_path)
    else:
        sec_path = rel_path

    return sec_path
