"""
This module provides components to expose the API of `JobInstance` objects via a local domain socket.
The main component is `APIServer`, which offers the addition or removal of `JobInstance`s using
the `add_job_instance()` and `remove_job_instance()` methods.

The domain socket with an `.api` file suffix for each server is located in the user's own subdirectory,
which is in the `/tmp` directory by default.
"""

import json
import logging
from abc import ABC, abstractmethod
from json import JSONDecodeError

from runtoolsio.runcore import paths
from runtoolsio.runcore.criteria import JobRunAggregatedCriteria
from runtoolsio.runcore.job import JobInstanceManager
from runtoolsio.runcore.run import util
from runtoolsio.runcore.util.socket import SocketServer

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name():
    return util.unique_timestamp_hex() + API_FILE_EXTENSION


class _ApiError(Exception):

    def __init__(self, code, error):
        self.code = code
        self.error = error

    def create_response(self):
        return _resp_err(self.code, self.error)


class APIResource(ABC):

    @property
    @abstractmethod
    def path(self):
        """Path of the resource including leading '/' character"""

    def validate(self, req_body):
        """Raise :class:`__ServerError if request body is invalid"""

    @abstractmethod
    def handle(self, job_instance, req_body):
        """Handle request and optionally return response or raise :class:`__ServerError"""


class InstancesResource(APIResource):

    @property
    def path(self):
        return '/instances'

    def handle(self, job_instance, req_body):
        return {"job_run": job_instance.job_run_info().serialize()}


class ApproveResource(APIResource):

    @property
    def path(self):
        return '/instances/approve'

    def validate(self, req_body):
        if 'phase' not in req_body:
            raise _missing_field_error('phase')

    def handle(self, job_instance, req_body):
        phase_name = req_body['phase']
        phase = job_instance.phases.get(phase_name)
        if not phase:
            return {"approval_result": 'NOT_APPLICABLE'}

        try:
            phase.approve()
        except AttributeError:
            return {"approval_result": 'NOT_APPLICABLE'}  # Or an error?

        return {"approval_result": 'APPROVED'}


class StopResource(APIResource):

    @property
    def path(self):
        return '/instances/stop'

    def handle(self, job_instance, req_body):
        job_instance.stop()
        return {"stop_result": "INITIATED"}


class OutputResource(APIResource):

    @property
    def path(self):
        return '/instances/output'

    def handle(self, job_instance, req_body):
        return {"output": job_instance.fetch_output()}  # TODO Limit length


class SignalProceedResource(APIResource):

    @property
    def path(self):
        return '/instances/_signal/dispatch'

    def handle(self, job_instance, req_body):
        waiter = job_instance.queue_waiter
        if waiter:
            executed = waiter.signal_dispatch()
        else:
            executed = False

        return {"waiter_found": waiter is not None, "executed": executed}


DEFAULT_RESOURCES = (
    InstancesResource(),
    ApproveResource(),
    StopResource(),
    OutputResource(),
    SignalProceedResource())


class APIServer(SocketServer, JobInstanceManager):

    def __init__(self, resources=DEFAULT_RESOURCES):
        super().__init__(lambda: paths.socket_path(_create_socket_name(), create=True), allow_ping=True)
        self._resources = {resource.path: resource for resource in resources}
        self._job_instances = []

    def register_instance(self, job_instance):
        self._job_instances.append(job_instance)

    def unregister_instance(self, job_instance):
        self._job_instances.remove(job_instance)

    def handle(self, req):
        try:
            req_body = json.loads(req)
        except JSONDecodeError as e:
            log.warning(f"event=[invalid_json_request_body] length=[{e}]")
            return _resp_err(400, "invalid_req_body")

        if 'request_metadata' not in req_body:
            return _resp_err(422, "missing_field:request_metadata")

        try:
            resource = self._resolve_resource(req_body)
            resource.validate(req_body)
            job_instances = self._matching_instances(req_body)
        except _ApiError as e:
            return e.create_response()

        instance_responses = []
        for job_instance in job_instances:
            # noinspection PyBroadException
            try:
                instance_response = resource.handle(job_instance, req_body)
            except _ApiError as e:
                return e.create_response()
            except Exception:
                log.error("event=[api_handler_error]", exc_info=True)
                return _resp_err(500, 'Unexpected API handler error')
            instance_response['instance_metadata'] = job_instance.metadata.serialize()
            instance_responses.append(instance_response)

        return _resp_ok(instance_responses)

    def _resolve_resource(self, req_body) -> APIResource:
        if 'api' not in req_body['request_metadata']:
            raise _missing_field_error('request_metadata.api')

        api = req_body['request_metadata']['api']
        resource = self._resources.get(api)
        if not resource:
            raise _ApiError(404, f"{api} API not found")

        return resource

    def _matching_instances(self, req_body):
        run_match = req_body.get('request_metadata', {}).get('run_match', None)
        if not run_match:
            return self._job_instances

        try:
            matching_criteria = JobRunAggregatedCriteria.deserialize(run_match)
        except ValueError:
            raise _ApiError(422, f"Invalid run match: {run_match}")
        return [job_instance for job_instance in self._job_instances if matching_criteria.matches(job_instance)]


def _missing_field_error(field) -> _ApiError:
    return _ApiError(422, f"Missing field {field}")


def _inst_metadata(job_instance):
    return {
        "job_id": job_instance.job_id,
        "instance_id": job_instance.run_id
    }


def _resp_ok(instance_responses):
    return _resp(200, instance_responses)


def _resp(code: int, instance_responses):
    resp = {
        "response_metadata": {"code": code},
        "instance_responses": instance_responses
    }
    return json.dumps(resp)


def _resp_err(code: int, reason: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")

    err_resp = {
        "response_metadata": {"code": code, "error": {"reason": reason}}
    }

    return json.dumps(err_resp)
