import oci
from oci.object_storage.models import (
    CreateBucketDetails, 
    CreatePreauthenticatedRequestDetails
)
from django.utils.deconstruct import deconstructible
from storages.base import BaseStorage
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import File
from storages.utils import setting
from django.utils.timezone import make_naive
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta


@deconstructible
class OracleObjectStorage(BaseStorage):

    def __init__(self, config_file=None, profile_name=None, bucket_name=None):
        if config_file is None:
            config_file = "~/.oci/config" or setting('ORACLE_OCI_CONFIG')

        profile_name = profile_name or setting('ORACLE_OCI_PROFILE')
        if profile_name is None:
            raise ImproperlyConfigured("Oracle OCI - No Profile Specified")

        self._bucket = bucket_name or setting('ORACLE_OCI_BUCKET')
        if self._bucket is None:
            raise ImproperlyConfigured("Oracle OCI - No Bucket Specified")
        
        self._config = oci.config.from_file(config_file,profile_name)
        self._object_storage = None
        
        self._connect()
        self._set_bucket()
        
    def __del__(self): 
        if self.connection is not None:
            self._disconnect()

    @property
    def connection(self):
        return self._object_storage

    @property
    def namespace(self):
        return self.connection.get_namespace().data

    @property
    def bucket(self):
        return self._bucket

    @property
    def regional_domain(self):
        return f"https://objectstorage.{self._config['region']}.oraclecloud.com"

    def _set_bucket(self):
        # try to create the bucket if it isnt there... just like s3
        try:
            response = self.connection.head_bucket(self.namespace,self.bucket)
        except oci.exceptions.ServiceError as error:
            print(error)
            if error.status == 404:
                # bucket not found
                request = CreateBucketDetails()
                request.compartment_id = self._config['tenancy']
                request.name = self.bucket
                response = self.connection.create_bucket(self.namespace, request)

        return response.data

    def _get_file_metadata(self,name):
        response = self.connection.list_objects(
            namespace_name=self.namespace,
            bucket_name=self._bucket,prefix=name
        )
        
        # oracle is weird, only filename, but no metadata is returned.
        # print(response.data.objects)

        if len(response.data.objects) == 1:
            return response.data.objects[0]
        return None

    def _open(self, name, mode='rb'):
        response = self.connection.get_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket, 
            object_name=name
        )
        return response.data

    def _save(self, name, content):
        response = self.connection.put_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket, 
            object_name=name, 
            put_object_body=content
        )
        return name

    def delete(self, name):
        self.connection.delete_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket, 
            object_name=name
        )

    def exists(self, name):
        if self._get_file_metadata(name) is None:
            return False
        return True

    def listdir(self):
        response = self.connection.list_objects(
            namespace_name=self.namespace,
            bucket_name=self.bucket
        )
        objects = [object.name for object in response.data.objects]
        return objects

    def size(self, name):
        properties = self._get_file_metadata(name)
        if properties is None:
            return None
        return properties.size

    def get_modified_time(self, name):
        properties = self._get_file_metadata(name)
        if properties is None:
            return None
        return make_naive(properties.time_modified)

    def modified_time(self, name):
        return self.get_modified_time(name)

    def url(self, name, parameters=None, expire=None, http_method=None):
        if expire is None:
            expire = datetime.now() + relativedelta(days=1)

        request = CreatePreauthenticatedRequestDetails()
        request.name = name
        request.object_name = name
        request.access_type = "ObjectRead"
        request.time_expires = expire
        response = self.connection.create_preauthenticated_request(
            namespace_name=self.namespace,
            bucket_name=self.bucket,
            create_preauthenticated_request_details=request
        )

        return f"{self.regional_domain}{response.data.access_uri}"

    def _connect(self):
        if self.connection is None:
            self._object_storage = oci.object_storage.ObjectStorageClient(self._config)
        return self.connection

    def _disconnect(self):
        if self.connection is not None:
            del self._object_storage
        

@deconstructible
class OracleObjectStorageFile(File):
    def __init__(self, name, storage, mode):
        self.name = name
        self._storage = storage
        self._mode = mode
        self._is_dirty = False
        self.file = io.BytesIO()
        self._is_read = False

    @property
    def size(self):
        if not hasattr(self, '_size'):
            self._size = self._storage.size(self.name)
        return self._size

    def readlines(self):
        if not self._is_read:
            self.file = self._storage._open(self.name)
            self._is_read = True
        return self.file.readlines()

    def read(self, num_bytes=None):
        if not self._is_read:
            self.file = self._storage._open(self.name)
            self._is_read = True
        return self.file.read(num_bytes)

    def write(self, content):
        if 'w' not in self._mode:
            raise AttributeError("File was opened for read-only access.")
        self.file = io.BytesIO(content)
        self._is_dirty = True
        self._is_read = True

    def close(self):
        if self._is_dirty:
            self._storage._save(self.name, self)
        self.file.close()
