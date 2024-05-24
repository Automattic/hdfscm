import nbformat
from notebook.services.contents.tests.test_contents_api import (
    assert_http_error, APITest
)
from traitlets.config import Config
from unicodedata import normalize
from hdfscm import HDFSContentsManager
from hdfscm.utils import to_fs_path
from pyarrow import fs

from .conftest import random_root_dir


class HDFSContentsAPITest(APITest):
    hidden_dirs = []
    root_dir = random_root_dir()
    config = Config()
    config.NotebookApp.contents_manager_class = HDFSContentsManager
    config.HDFSContentsManager.root_dir = root_dir

    @classmethod
    def setup_class(cls):
        """Due to https://github.com/docker/for-linux/issues/250, tornado maps
        localhost to an unresolvable ipv6 address. The easiest way to workaround
        this is to make it look like python was built without ipv6 support. This
        patch could fail if `tornado.netutils.bind_sockets` is updated. Note
        that this doesn't indicate a problem with real world use."""
        import socket
        cls._has_ipv6 = socket.has_ipv6
        socket.has_ipv6 = False
        super().setup_class()

    @classmethod
    def teardown_class(cls):
        """See setUpClass above"""
        import socket
        socket.has_ipv6 = cls._has_ipv6
        super().teardown_class()

    def setUp(self):
        self.notebook.contents_manager.ensure_root_directory()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.fs.delete_dir(self.root_dir)

    @property
    def fs(self):
        return self.notebook.contents_manager.fs

    def get_hdfs_path(self, api_path):
        return to_fs_path(api_path, self.root_dir)

    def make_dir(self, api_path):
        self.fs.create_dir(self.get_hdfs_path(api_path))

    def make_blob(self, api_path, blob):
        hdfs_path = self.get_hdfs_path(api_path)
        with self.fs.open_output_stream(hdfs_path) as f:
            f.write(blob)

    def make_txt(self, api_path, txt):
        self.make_blob(api_path, txt.encode('utf-8'))

    def make_nb(self, api_path, nb):
        self.make_txt(api_path, nbformat.writes(nb, version=4))

    def delete_file(self, api_path):
        hdfs_path = self.get_hdfs_path(api_path)
        if self.fs.get_file_info(hdfs_path).type == fs.FileType.Directory:
            self.fs.delete_dir(hdfs_path)
        elif self.fs.get_file_info(hdfs_path).type == fs.FileType.File:
            self.fs.delete_file(hdfs_path)

    delete_dir = delete_file

    def isfile(self, api_path):
        return self.fs.get_file_info(self.get_hdfs_path(api_path)).type == fs.FileType.File

    def isdir(self, api_path):
        return self.fs.get_file_info(self.get_hdfs_path(api_path)).type == fs.FileType.Directory

    # Test overrides.
    def test_checkpoints_separate_root(self):
        pass

    def test_delete_non_empty_dir(self):
        with assert_http_error(400):
            self.api.delete('å b')

    def dirs_only(self, dir_model):
        return [x for x in dir_model['content'] if x['type']=='directory']

    def test_list_dirs(self):
        dirs = self.dirs_only(self.api.list().json())
        dir_names = {normalize('NFC', d['name']) for d in dirs}
        top_level_dirs = self.top_level_dirs
        self.assertEqual(dir_names, top_level_dirs)  # Excluding hidden dirs

    def test_delete_dirs(self):
        # depth-first delete everything, so we don't try to delete empty directories
        for name in sorted(self.dirs + ['/'], key=len, reverse=True):
            listing = self.api.list(name).json()['content']
            for model in listing:
                self.api.delete(model['path'])
        listing = [file['path'] for file in self.api.list('/').json()['content']]
        expected = []
        self.assertEqual(listing, expected)


del APITest
