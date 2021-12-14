import io
import logging
from typing import Union
from pathlib import Path
from .file_system import FileSystem
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, FileSystemClient
import re

_logger = logging.getLogger(__name__)

ABFSS_CLIENT = None
CONF = None

def url_to_container_datalake_name_tuple(url: Union[Path, str]):
    result = re.search(r"abfss:\/(\w+)@([\w\.]+).dfs.core.windows.net\/([\w\-\_\/\.]+)", str(url))
    if result is not None:
        return result.group(1), result.group(2), result.group(3)
    raise ValueError(f"abfss url '{url}'' not matching regex.")

class AbfssFileSystem(FileSystem):
    STORAGE_ACCOUNT_NAME = "abfss_storage_account_name"
    STORAGE_ACCOUNT_KEY = "abfss_storage_account_key"
    _singleton = None

    @staticmethod
    def get_instance():
        if AbfssFileSystem._singleton is None:
            AbfssFileSystem()
        return AbfssFileSystem._singleton

    def __init__(self: "AbfssFileSystem") -> None:
        if AbfssFileSystem._singleton is None:
            AbfssFileSystem._singleton = self

    def set_conf(self, conf: dict):
        global CONF
        CONF = conf

    def open(self, path, mode='rb'):
        return AbfssFile(path, mode=mode)

    def exists(self, path):
        print(f"Looking for abfss path {path}")
        file_client = AbfssFileSystem.get_instance().getFileClient(path)
        try:
            file_client.get_file_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as error:
            print(error)    
            raise

    def stat(self, path):
        raise NotImplementedError()

    def __get_abfss_service_client(self):
        try:  
            global ABFSS_CLIENT
            if ABFSS_CLIENT is None:
                ABFSS_CLIENT = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                    "https", CONF.get(AbfssFileSystem.STORAGE_ACCOUNT_NAME)), credential=CONF.get(AbfssFileSystem.STORAGE_ACCOUNT_KEY))
            return ABFSS_CLIENT        
        except Exception as e:
            _logger.error(e)
            raise

    def getFileClient(self, path):
        try:  
            # file_client = DataLakeFileClient(account_url="{}://{}.dfs.core.windows.net".format(
            #     "https", CONF.get(AbfssFileSystem.STORAGE_ACCOUNT_NAME)), 
            #     file_system_name=file_system_name,
            #     file_path=file_path,
            #     credential=CONF.get(AbfssFileSystem.STORAGE_ACCOUNT_KEY))
            # return file_client
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
        self.storageStream = self.file_client.download_file()
        if mode.startswith("r"):
            self.size = self.storageStream.properties.size
        self.closed = False
        self.mode = mode
        _logger.info(f"Created a AbfssFile for {path}: {self.size}")

    def readline(self, n=0):
        if self.curr_buffer is None:
            self.curr_buffer = io.BytesIO(self.storageStream.readall())
        for line in self.curr_buffer:
            yield line
            # with io.BytesIO() as b:
            #     self.storageStream.readinto(b)
            #     b.seek(0)
            #     # print(str(b.read(4)))
            #     for line in b: #self.curr_buffer:
            #         print("******************************************************************************")
            #         print(line)
            #         print("******************************************************************************")
            #         yield line

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __next__(self):
        return next(self.readline())

    def __iter__(self):
        return self

    def close(self):
        self.closed = True

    def flush(self):
        pass
