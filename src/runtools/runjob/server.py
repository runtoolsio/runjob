import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from json import JSONDecodeError
from typing import Dict, Any, List, Union

from itertools import zip_longest

from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobInstanceManager, JobInstance
from runtools.runcore.util.json import ErrorCode, JsonRpcError
from runtools.runcore.util.socket import SocketServer

log = logging.getLogger(__name__)

RPC_FILE_EXTENSION = '.rpc'


@dataclass
class MethodParameter:
    """Defines a parameter for a JSON-RPC method"""
    name: str
    param_type: type
    required: bool = True
    default: Any = None


RUN_MATCH_PARAM = MethodParameter('run_match', dict)
INSTANCE_ID_PARAM = MethodParameter('instance_id', str)


class JsonRpcMethodType(Enum):
    COLLECTION = auto()
    INSTANCE = auto()


class JsonRpcMethod(ABC):
    """Base class for JSON-RPC methods with parameter validation"""

    @property
    @abstractmethod
    def type(self) -> JsonRpcMethodType:
        """Defines whether the method operates on a collection of instances or a single instance"""
        pass

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


class GetActiveRunsMethod(JsonRpcMethod):

    @property
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.COLLECTION

    @property
    def method_name(self):
        return "get_active_runs"

    @property
    def parameters(self):
        return [RUN_MATCH_PARAM]

    def execute(self, job_instances):
        return [i.snapshot().serialize() for i in job_instances]


class StopInstanceMethod(JsonRpcMethod):

    @property
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    def method_name(self):
        return "stop_instance"

    @property
    def parameters(self):
        return [INSTANCE_ID_PARAM]

    def execute(self, job_instance):
        job_instance.stop()


class GetOutputTailMethod(JsonRpcMethod):

    @property
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    def method_name(self):
        return "get_output_tail"

    @property
    def parameters(self):
        return [INSTANCE_ID_PARAM, MethodParameter("max_lines", int, required=False, default=100)]

    def execute(self, job_instance, max_lines):
        return [line.serialize() for line in job_instance.output.tail(max_lines=max_lines)]


class ExecPhaseOpMethod(JsonRpcMethod):

    @property
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    def method_name(self) -> str:
        return "exec_phase_op"

    @property
    def parameters(self):
        return [
            INSTANCE_ID_PARAM,
            MethodParameter("phase_id", str, required=True),
            MethodParameter("op_name", str, required=True),
            MethodParameter("op_args", list, required=False, default=[])
        ]

    def execute(self, job_instance: JobInstance, phase_id, op_name, op_args) -> Dict[str, Any]:
        control = job_instance.find_phase_control_by_id(phase_id)
        if not control:
            raise JsonRpcError(ErrorCode.PHASE_NOT_FOUND, f"Phase not found: {phase_id}")

        operation = getattr(control, op_name, None)
        if operation is None:
            raise JsonRpcError(ErrorCode.PHASE_OP_NOT_FOUND, f"Phase operation not found: {op_name}")
        try:
            result = operation(*op_args)
        except AttributeError as e:
            raise JsonRpcError(ErrorCode.METHOD_NOT_FOUND, str(e))
        except TypeError as e:
            raise JsonRpcError(ErrorCode.PHASE_OP_INVALID_ARGS, f"Invalid arguments for operation: {str(e)}")

        return {"retval": str(result)}


DEFAULT_METHODS = (
    GetActiveRunsMethod(),
    StopInstanceMethod(),
    GetOutputTailMethod(),
    ExecPhaseOpMethod()
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
            "code": code.int_code,
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


class RemoteCallServer(SocketServer, JobInstanceManager):
    """
    JSON-RPC 2.0 API Server that handles requests for job instances.

    Each instance method requires an instance_id parameter, while collection methods use
    run_match parameter to identify target job instances. The run_match follows
    JobRunCriteria serialization format.

    Examples:
        # Collection method - get_active_runs (example using dict params)
        --> {"jsonrpc": "2.0", "id": 1, "method": "get_active_runs", "params": {"run_match": {...}}}
        <-- {"jsonrpc": "2.0", "id": 1, "result": {"retval": [{"job_id": "job123", ...}]}}

        # Instance method - get_output_tail (example using list params)
        --> {"jsonrpc": "2.0", "id": 2, "method": "get_output_tail", "params": ["inst789", 2]}
        <-- {"jsonrpc": "2.0", "id": 2,
             "result": {"retval": [
               {"text": "Processing started", "is_error": false},
               {"text": "Step 1 complete", "is_error": false}
             ]}}

        # Error response
        <-- {"jsonrpc": "2.0", "id": 3, "error": {"code": -32001, "message": "Instance not found: inst999"}}
    """
    def __init__(self, socket_path, methods=DEFAULT_METHODS):
        super().__init__(socket_path, allow_ping=True)
        self._methods = {method.method_name: method for method in methods}
        self._job_instances = {}

    def register_instance(self, job_instance):
        self._job_instances[job_instance.instance_id] = job_instance

    def unregister_instance(self, job_instance):
        del self._job_instances[job_instance.instance_id]

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
            validated_args = validate_params(method.parameters, params)
            if method.type == JsonRpcMethodType.INSTANCE:
                try:
                    job_instance = self._job_instances[validated_args[0]]
                except KeyError:
                    return _error_response(request_id, ErrorCode.TARGET_NOT_FOUND,
                                           f"Instance not found: {validated_args[0]}")
                exec_retval = method.execute(job_instance, *validated_args[1:])
            elif method.type == JsonRpcMethodType.COLLECTION:
                job_instances = self._matching_instances(validated_args[0])
                exec_retval = method.execute(job_instances, *validated_args[1:])
            else:
                raise AssertionError("Missing implementation for method type: " + str(method.type))
        except JsonRpcError as e:
            return _error_response(request_id, e.code, e.message, e.data)
        except Exception as e:
            log.error("event=[json_rpc_handler_error]", exc_info=True)
            return _error_response(request_id, ErrorCode.METHOD_EXECUTION_ERROR, f"Internal error: {str(e)}")

        return _success_response(request_id, {"retval": exec_retval})

    def _matching_instances(self, run_match: Dict) -> List:
        try:
            matching_criteria = JobRunCriteria.deserialize(run_match)
        except ValueError as e:
            raise JsonRpcError(ErrorCode.INVALID_PARAMS, f"Invalid run match criteria: {e}")

        return [job_instance for job_instance in self._job_instances.values() if matching_criteria.matches(job_instance.snapshot())]
