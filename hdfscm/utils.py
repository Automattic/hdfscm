from contextlib import contextmanager
from datetime import datetime, tzinfo, timedelta

from pyarrow import ArrowIOError
from tornado.web import HTTPError


_ZERO = timedelta(0)


class _utc_tzinfo(tzinfo):
    def utcoffset(self, d):
        return _ZERO

    dst = utcoffset


_UTC = _utc_tzinfo()


def utcnow():
    return datetime.now().replace(tzinfo=_UTC)


def to_api_path(fs_path, root):
    if fs_path.startswith(root):
        fs_path = fs_path[len(root):]
    parts = fs_path.strip('/').split('/')
    parts = [p for p in parts if p != '']  # remove duplicate splits
    return '/'.join(parts)


def to_fs_path(path, root):
    parts = [root]
    split = path.strip('/').split('/')
    parts.extend(p for p in split if p != '')  # remove duplicate splits
    fs_path = '/'.join(parts)
    if not fs_path.startswith(root):
        raise HTTPError(404, "%s is outside root directory" % path)
    return fs_path


def is_hidden(fs_path, root):
    path = to_api_path(fs_path, root)
    return any(part.startswith('.') for part in path.split("/"))

def get_prefix_from_fs_path(path, root_dir, shared_dir):
    """
    Returns the path prefix for the given path after resolving if it should be
    served from the global shared dir or personal notebook dir.
    """
    if path.strip('/').split('/')[0] == 'shared':
        return shared_dir
    else:
        return root_dir

def get_prefix_from_hdfs_path(path, root_dir, shared_dir):
    """
    Returns the path prefix for the given path after resolving if the HDFS dir
    is a personal notebook dir or the shared dir.
    """
    if path.startswith(shared_dir):
        return shared_dir
    else:
        return root_dir

@contextmanager
def perm_to_403(path):
    try:
        yield
    except ArrowIOError as exc:
        # For now we can't access the errno attribute of the error directly,
        # detect it from the string instead.
        if 'errno: 13 (Permission denied)' in str(exc):
            raise HTTPError(403, 'Permission denied: %s' % path)
