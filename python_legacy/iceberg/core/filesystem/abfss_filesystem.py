import io
import logging
import re
from pathlib import Path, PosixPath
from typing import Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import (DataLakeFileClient,
                                        DataLakeServiceClient,
                                        FileSystemClient)
from iceberg.exceptions.exceptions import FileSystemNotFound

from .file_status import FileStatus
from .file_system import FileSystem

_logger = logging.getLogger(__name__)

ABFSS_CLIENT = None
CONF = None

def url_to_container_datalake_name_tuple(url: Union[Path, str]):
    result = re.search(r"abfss:\/[\/]?(\w+)@([\w\.]+).dfs.core.windows.net\/([\w\-\_\/\.]+)", str(url))
    if result is not None:
        return result.group(1), result.group(2), result.group(3)
    raise ValueError(f"Invalid abfss url: {url}")

class AbfssFileSystem(FileSystem):
    ACCOUNT_NAME = "account_name"
    ACCOUNT_KEY = "account_key"
    SAS_TOKEN = "sas_token"
    fs_inst = None

    @staticmethod
    def get_instance():
        if AbfssFileSystem.fs_inst is None:
            AbfssFileSystem()
        return AbfssFileSystem.fs_inst

    def __init__(self: "AbfssFileSystem") -> None:
        if AbfssFileSystem.fs_inst is None:
            AbfssFileSystem.fs_inst = self

    def set_conf(self, conf: dict):
        global CONF
        CONF = conf

    def _get_credentials(self):
        return CONF.get(AbfssFileSystem.SAS_TOKEN) if (AbfssFileSystem.SAS_TOKEN in CONF) else CONF.get(AbfssFileSystem.ACCOUNT_KEY)

    def exists(self, path):
        file_client = AbfssFileSystem.get_instance().getFileClient(path)
        try:
            file_client.get_file_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as error:
            raise

    def open(self, path, mode='rb'):
        return AbfssFile(path, mode=mode)

    def delete(self, path):
        file_client = AbfssFileSystem.get_instance().getFileClient(path)
        file_client.delete_file()

    def stat(self, path):
        file_client = AbfssFileSystem.get_instance().getFileClient(path)
        fp = file_client.get_file_properties()

        return FileStatus(path=path, length=fp.size, is_dir=False,
                          blocksize=None, modification_time=fp.last_modified, access_time=None,
                          permission=None, owner=None, group=None)

    def create(self, path, overwrite=False):
        return AbfssFile(path, "w")

    def rename(self, src, dest):
        file_client = AbfssFileSystem.get_instance().getFileClient(src)
        container, datalake, path = url_to_container_datalake_name_tuple(dest)
        new_name = f"{file_client.file_system_name}/{path}"
        file_client.rename_file(new_name)
        return AbfssFile(dest, mode='r')

    def __get_abfss_service_client(self):
        try:  
            global ABFSS_CLIENT
            if ABFSS_CLIENT is None:
                ABFSS_CLIENT = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                    "https", CONF.get(AbfssFileSystem.ACCOUNT_NAME)), credential=self._get_credentials())
            return ABFSS_CLIENT        
        except Exception as e:
            _logger.error(e)
            raise

    def getFileClient(self, path):
        try:  
            container, datalake, name = url_to_container_datalake_name_tuple(path)
            return self.__get_abfss_service_client().get_file_client(container, name)
        except Exception as e:
            _logger.error(e)
            raise

    def getFileSystemClient(self, container: str) -> FileSystemClient:
        try:  
            return self.__get_abfss_service_client().get_file_system_client(container)
        except Exception as e:
            _logger.error(e)
            raise

class AbfssFile(object):

    def __init__(self, path, mode="rb") -> None:
        self.curr_buffer = None
        self.path = path
        self.file_client = AbfssFileSystem.get_instance().getFileClient(path)
        if mode.startswith("r"):
            try:
                self.storageStreamDownloader = self.file_client.download_file(0)
            except ResourceNotFoundError as e:
                # TODO: find or create a better exception for this.  Don't want to raise Azure exception.
                raise FileSystemNotFound(f"Azure File not found: {self.path}") from e
            self.size = self.storageStreamDownloader.properties.size
        self.closed = False
        self.mode = mode

    def __check_read_access__(self):
        if not self.mode.startswith("r"):
            raise RuntimeError("Cannot read from AbfssFile, not open in 'r' mode")

    def __check_write_access__(self):
        if not self.mode.startswith("w"):
            raise RuntimeError("Cannot write to AbfssFile, not open in 'w' mode")

    def read(self, n=0):
        self.__check_read_access__()
        if self.curr_buffer is None:
            self.curr_buffer = io.BytesIO()
            self.storageStreamDownloader.readinto(self.curr_buffer)
            self.curr_buffer.seek(0)
        return self.curr_buffer.read(n)

    def readline(self, n=0):
        self.__check_read_access__()
        if self.curr_buffer is None:
            self.curr_buffer = io.BytesIO(self.storageStreamDownloader.readall())
        for line in self.curr_buffer:
            yield line

    def write(self, string):
        self.__check_write_access__()
        dataLength = len(string)
        self.file_client.create_file()
        self.file_client.append_data(data=string, offset=0, length=dataLength)
        self.file_client.flush_data(dataLength)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __next__(self):
        return next(self.readline())

    def __iter__(self):
        return self

    def close(self):
        if self.curr_buffer is not None:
            self.curr_buffer.close()
        self.closed = True

    def flush(self):
        pass
