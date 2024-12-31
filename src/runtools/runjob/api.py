import json
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from json import JSONDecodeError
from typing import Optional, Dict, Any, List

from runtools.runcore import paths
from runtools.runcore.client import StopResult
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobInstanceManager
from runtools.runcore.run import util
from runtools.runcore.util.socket import SocketServer
from runtools.runjob.coord import CoordTypes

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


class ErrorCode(IntEnum):
    # Standard JSON-RPC 2.0 errors
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    # Custom error codes
    INVALID_RUN_MATCH = -32000


def _create_socket_name():
    return util.unique_timestamp_hex() + API_FILE_EXTENSION


class JsonRpcError(Exception):

    def __init__(self, code: ErrorCode, message: str, data: Optional[Any] = None):
        self.code = code
        self.message = message
        self.data = data


class JsonRpcMethod(ABC):

    @property
    @abstractmethod
    def method_name(self) -> str:
        """JSON-RPC method name including namespace prefix"""

    @abstractmethod
    def execute(self, job_instance, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the method and return response or raise JsonRpcError"""

    def requires_run_match_param(self) -> bool:
        return False

    def validate_params(self, params: Dict[str, Any]):
        """Validate parameters, raise JsonRpcError if invalid"""


class InstancesGetMethod(JsonRpcMethod):

    @property
    def method_name(self):
        return "instances.get"

    def execute(self, job_instance, params):
        return {"job_run": job_instance.snapshot().serialize()}


class InstancesApproveMethod(JsonRpcMethod):

    @property
    def method_name(self):
        return "instances.approve"

    def execute(self, job_instance, params):
        if phase_id := params.get('phase_id'):
            try:
                phase = job_instance.get_phase_control(phase_id)
            except KeyError:
                return {"approved": False, "reason": f"Phase `{phase_id}` to approve not found"}
        else:
            phase = job_instance.current_phase

        try:
            phase.approve()
        except AttributeError:
            return {"approved": False, "reason": f"Phase `{phase_id}` does not support approval"}

        return {"approved": True}

    def requires_run_match_param(self) -> bool:
        return True


class InstancesStopMethod(JsonRpcMethod):

    @property
    def method_name(self):
        return "instances.stop"

    def execute(self, job_instance, params):
        job_instance.stop()
        return {"stop_result": StopResult.STOP_INITIATED.name}


class InstancesTailMethod(JsonRpcMethod):

    @property
    def method_name(self):
        return "instances.get_tail"

    def execute(self, job_instance, params):
        return {"tail": [line.serialize() for line in job_instance.output.tail()]}


class InstancesDispatchMethod(JsonRpcMethod):

    @property
    def method_name(self):
        return "instances.dispatch"

    def validate_params(self, params):
        if "queue_id" not in params:
            raise JsonRpcError(ErrorCode.INVALID_PARAMS, "Missing required parameter: queue_id")

    def execute(self, job_instance, params):
        dispatched = False
        for phase in job_instance.phases.values():
            if phase.type == CoordTypes.QUEUE.value and phase.queue_id == params['queue_id']:
                dispatched = phase.signal_dispatch()
                break

        if not dispatched:
            return {"dispatched": False}

        return {"dispatched": True}

    def requires_run_match_param(self) -> bool:
        return True


DEFAULT_METHODS = (
    InstancesGetMethod(),
    InstancesApproveMethod(),
    InstancesStopMethod(),
    InstancesTailMethod(),
    InstancesDispatchMethod()
)


def _is_valid_request_id(request_id: Any) -> bool:
    return request_id is None or isinstance(request_id, (str, int, float))

def _is_valid_params(params: Any) -> bool:
    return 'params' is None or isinstance(params, (dict, list))


def _success_response(request_id: str, result: Any) -> str:
    response = {
        "jsonrpc": "2.0",
        "result": result
    }
    if request_id:
        response["id"] = request_id
    return json.dumps(response)


def _error_response(request_id: Any, code: ErrorCode, message: str, data: Any = None) -> str:
    response = {
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message
        }
    }
    if data:
        response["error"]["data"] = data
    if request_id:
        response["id"] = request_id
    return json.dumps(response)


class APIServer(SocketServer, JobInstanceManager):

    def __init__(self, methods=DEFAULT_METHODS):
        super().__init__(lambda: paths.socket_path(_create_socket_name(), create=True), allow_ping=True)
        self._methods = {method.method_name: method for method in methods}
        self._job_instances = []

    def register_instance(self, job_instance):
        self._job_instances.append(job_instance)

    def unregister_instance(self, job_instance):
        self._job_instances.remove(job_instance)

    def handle(self, req: str) -> str:
        try:
            req_data = json.loads(req)
        except JSONDecodeError:
            return _error_response(None, ErrorCode.PARSE_ERROR, "Invalid JSON")

        # Validate JSON-RPC request
        if not isinstance(req_data, dict) or req_data.get('jsonrpc') != '2.0' or 'method' not in req_data:
            return _error_response(req_data.get('id'), ErrorCode.INVALID_REQUEST, "Invalid JSON-RPC 2.0 request")

        request_id = req_data.get('id')
        if not _is_valid_request_id(request_id):
            return _error_response(req_data.get('id'), ErrorCode.INVALID_REQUEST, "Invalid request ID")

        params = req_data.get('params', {})
        if not _is_valid_params(params):
            return _error_response(req_data.get('id'), ErrorCode.INVALID_REQUEST, "Invalid parameters")

        method_name = req_data['method']
        method = self._methods.get(method_name)
        if not method:
            return _error_response(request_id, ErrorCode.METHOD_NOT_FOUND, f"Method not found: {method_name}")

        try:
            method.validate_params(params)
            job_instances = self._matching_instances(params, method.requires_run_match_param())
            result = []
            for job_instance in job_instances:
                exec_result = method.execute(job_instance, params)
                exec_result['instance_metadata'] = job_instance.metadata.serialize()
                result.append(exec_result)

            return _success_response(request_id, result)

        except JsonRpcError as e:
            return _error_response(request_id, e.code, e.message, e.data)
        except Exception as e:
            log.error("event=[json_rpc_handler_error]", exc_info=True)
            return _error_response(request_id, ErrorCode.INTERNAL_ERROR, f"Internal error: {str(e)}")

    def _matching_instances(self, params: Dict[str, Any], match_required: bool) -> List:
        run_match = params.get('run_match')
        if not run_match:
            if match_required:
                raise JsonRpcError(ErrorCode.INVALID_PARAMS, "Missing required parameter: `run_match`")
            return self._job_instances

        try:
            matching_criteria = JobRunCriteria.deserialize(run_match)
        except ValueError as e:
            raise JsonRpcError(ErrorCode.INVALID_RUN_MATCH, f"Invalid run match criteria: {e}")

        return [job_instance for job_instance in self._job_instances if matching_criteria.matches(job_instance)]
