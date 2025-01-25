import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from itertools import zip_longest
from json import JSONDecodeError
from typing import Optional, Dict, Any, List, Union

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


@dataclass
class MethodParameter:
    """Defines a parameter for a JSON-RPC method"""
    name: str
    param_type: type
    required: bool = True
    default: Any = None


RUN_MATCH_PARAM = MethodParameter('run_match', dict)


class JsonRpcMethod(ABC):
    """Base class for JSON-RPC methods with parameter validation"""

    @property
    @abstractmethod
    def method_name(self) -> str:
        """JSON-RPC method name including namespace prefix"""
        pass

    @property
    def parameters(self) -> List[MethodParameter]:
        """Define the parameters this method accepts"""
        return []

    @abstractmethod
    def execute(self, *args) -> Dict[str, Any]:
        """Execute the method with validated parameters"""
        pass


class InstancesGetMethod(JsonRpcMethod):
    @property
    def method_name(self):
        return "instances.get"

    def execute(self, job_instance):
        return {"job_run": job_instance.snapshot().serialize()}


class InstancesApproveMethod(JsonRpcMethod):
    @property
    def method_name(self):
        return "instances.approve"

    @property
    def parameters(self):
        return [
            MethodParameter("phase_id", str, required=False),
        ]

    def execute(self, job_instance, phase_id):
        if phase_id:
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


class InstancesStopMethod(JsonRpcMethod):
    @property
    def method_name(self):
        return "instances.stop"

    def execute(self, job_instance):
        job_instance.stop()
        return {"stop_result": StopResult.STOP_INITIATED.name}


class InstancesTailMethod(JsonRpcMethod):
    @property
    def method_name(self):
        return "instances.get_tail"

    @property
    def parameters(self):
        return [
            MethodParameter("max_lines", int, required=False, default=100)
        ]

    def execute(self, job_instance, max_lines):
        return {"tail": [line.serialize() for line in job_instance.output.tail()]}


class InstancesDispatchMethod(JsonRpcMethod):
    @property
    def method_name(self):
        return "instances.dispatch"

    @property
    def parameters(self):
        return [
            MethodParameter("queue_id", str, required=True),
        ]

    def execute(self, job_instance, queue_id):
        dispatched = False
        for phase in job_instance.phases.values():
            if phase.type == CoordTypes.QUEUE.value and phase.queue_id == queue_id:
                dispatched = phase.signal_dispatch()
                break

        if not dispatched:
            return {"dispatched": False}

        return {"dispatched": True}


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
    return params is None or isinstance(params, (dict, list))


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


def validate_params(parameters, arguments: Union[List, Dict[str, Any]]) -> List[Any]:
    """
    Validate and transform input parameters according to method specification.
    Supports both positional (list) and named (dict) parameters.

    Args:
        parameters: The parameters of the method for which the arguments were provided
        arguments: Input parameters as either list (positional) or dict (named)

    Returns:
        List of validated parameters in the order defined by method.parameters

    Raises:
        JsonRpcError: If parameters are invalid
    """
    name_to_param = {p.name: p for p in parameters}
    validated_args = []

    # Convert named arguments to positional
    if isinstance(arguments, dict):
        if unknown_params := (set(arguments.keys()) - {'run_match'}) - set(name_to_param.keys()):
            raise JsonRpcError(ErrorCode.INVALID_PARAMS, f"Unknown parameters: {', '.join(unknown_params)}")

        arguments = [arguments.get(param.name) for param in parameters]

    for param, value in zip_longest(parameters, arguments):
        if param is None:
            raise JsonRpcError(
                ErrorCode.INVALID_PARAMS,
                f"Too many parameters. Expected {len(parameters)}, got {len(arguments)}"
            )

        if value is None:
            if param.required and param.default is None:
                raise JsonRpcError(ErrorCode.INVALID_PARAMS, f"Missing required parameter: {param.name}")
            validated_args.append(param.default)
        elif not isinstance(value, param.param_type):
            raise JsonRpcError(
                ErrorCode.INVALID_PARAMS,
                f"Parameter {param.name} must be of type {param.param_type.__name__}"
            )
        else:
            validated_args.append(value)

    return validated_args


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
            return _error_response(request_id, ErrorCode.INVALID_REQUEST, "Invalid request ID")

        params = req_data.get('params', {})
        if not _is_valid_params(params):
            return _error_response(request_id, ErrorCode.INVALID_REQUEST, "Invalid parameters")

        method_name = req_data['method']
        method = self._methods.get(method_name)
        if not method:
            return _error_response(request_id, ErrorCode.METHOD_NOT_FOUND, f"Method not found: {method_name}")

        try:
            validated_args = validate_params([RUN_MATCH_PARAM] + method.parameters, params)
            job_instances = self._matching_instances(validated_args[0])

            result = []
            for job_instance in job_instances:
                exec_result = method.execute(job_instance, *validated_args[1:])
                exec_result['instance_metadata'] = job_instance.metadata.serialize()
                result.append(exec_result)

            return _success_response(request_id, result)

        except JsonRpcError as e:
            return _error_response(request_id, e.code, e.message, e.data)
        except Exception as e:
            log.error("event=[json_rpc_handler_error]", exc_info=True)
            return _error_response(request_id, ErrorCode.INTERNAL_ERROR, f"Internal error: {str(e)}")

    def _matching_instances(self, run_match: Dict) -> List:
        try:
            matching_criteria = JobRunCriteria.deserialize(run_match)
        except ValueError as e:
            raise JsonRpcError(ErrorCode.INVALID_RUN_MATCH, f"Invalid run match criteria: {e}")

        return [job_instance for job_instance in self._job_instances if matching_criteria.matches(job_instance)]