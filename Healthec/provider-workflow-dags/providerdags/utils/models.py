import json
from datetime import datetime


class EventMessage(object):
    def __init__(self, event, status):
        self._event = event
        self._event_type = "file"
        self._version = "v1"
        self._event_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        self._origin = ""
        self._trace_id = ""
        self._user_id = ""
        self._status = status
        self._status_code = ""
        self._tenant_id = ""
        self._payer = ""
        self._resource_id = ""
        self._resource_type = ""
        self._links = {}
        self._file_name = ""
        self._batch_id = ""

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, value):
        self._origin = value

    @property
    def trace_id(self):
        return self._trace_id

    @trace_id.setter
    def trace_id(self, value):
        self._trace_id = value

    @property
    def user_id(self):
        return self._user_id

    @user_id.setter
    def user_id(self, value):
        self._user_id = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def status_code(self):
        return self._status_code

    @status_code.setter
    def status_code(self, value):
        self._status_code = value

    @property
    def tenant_id(self):
        return self._tenant_id

    @tenant_id.setter
    def tenant_id(self, value):
        self._tenant_id = value

    @property
    def payer(self):
        return self._payer

    @payer.setter
    def payer(self, value):
        self._payer = value

    @property
    def resource_id(self):
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value):
        self._resource_id = value

    @property
    def resource_type(self):
        return self._resource_type

    @resource_type.setter
    def resource_type(self, value):
        self._resource_type = value

    @property
    def links(self):
        return self._links

    @links.setter
    def links(self, value):
        self._links = value

    @property
    def file_name(self):
        return self._file_name

    @file_name.setter
    def file_name(self, value):
        self._file_name = value

    @property
    def batch_id(self):
        return self._batch_id

    @batch_id.setter
    def batch_id(self, value):
        self._batch_id = value

    def to_dict(self):
        return {
            "event": self._event,
            "eventType": self._event_type,
            "version": self._version,
            "eventTimeStamp": self._event_timestamp,
            "origin": self._origin,
            "traceId": self._trace_id,
            "userId": self._user_id,
            "status": self._status,
            "statusCode": self._status_code,
            "tenantId": self._tenant_id,
            "payer": self._payer,
            "resourceId": self._resource_id,
            "resourceType": self._resource_type,
            "links": self._links,
            "file": self._file_name,
            "batchId": self._batch_id,
        }

    def __str__(self):
        return json.dumps(self.to_dict())
