import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List, Union, override

from itertools import zip_longest
from runtools.runcore.job import InstanceID, JobInstance
from runtools.runcore.run import StopReason
from runtools.runcore.util.json import ErrorCode, JsonRpcError

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
INSTANCE_ID_PARAM = MethodParameter('instance_id', dict)
STOP_REASON_PARAM = MethodParameter('stop_reason', str, required=False, default="STOPPED")


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
    @override
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.COLLECTION

    @property
    @override
    def method_name(self):
        return "get_active_runs"

    @property
    @override
    def parameters(self):
        return [RUN_MATCH_PARAM]

    @override
    def execute(self, job_instances):
        return [i.snap().serialize() for i in job_instances]


class StopInstanceMethod(JsonRpcMethod):

    @property
    @override
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    @override
    def method_name(self):
        return "stop_instance"

    @property
    @override
    def parameters(self):
        return [INSTANCE_ID_PARAM, STOP_REASON_PARAM]

    @override
    def execute(self, job_instance, stop_reason_name: str = "STOPPED"):
        stop_reason = StopReason[stop_reason_name]
        job_instance.stop(stop_reason)


class GetOutputTailMethod(JsonRpcMethod):

    @property
    @override
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    @override
    def method_name(self):
        return "get_output_tail"

    @property
    @override
    def parameters(self):
        return [
            INSTANCE_ID_PARAM,
            MethodParameter("max_lines", int, required=False, default=100),
        ]

    @override
    def execute(self, job_instance, max_lines):
        return [line.serialize() for line in job_instance.output.tail(max_lines=max_lines)]


class ExecPhaseOpMethod(JsonRpcMethod):

    @property
    @override
    def type(self) -> JsonRpcMethodType:
        return JsonRpcMethodType.INSTANCE

    @property
    @override
    def method_name(self) -> str:
        return "exec_phase_op"

    @property
    @override
    def parameters(self):
        return [
            INSTANCE_ID_PARAM,
            MethodParameter("phase_id", str, required=True),
            MethodParameter("op_name", str, required=True),
            MethodParameter("op_args", list, required=False, default=[])
        ]

    @override
    def execute(self, job_instance: JobInstance, phase_id, op_name, op_args) -> Dict[str, Any]:
        control = job_instance.find_phase_control_by_id(phase_id)
        if not control:
            raise JsonRpcError(ErrorCode.PHASE_NOT_FOUND, f"Phase not found: {phase_id}")

        operation = getattr(control, op_name, None)
        if operation is None:
            raise JsonRpcError(
                ErrorCode.PHASE_OP_NOT_FOUND,
                f"Phase '{phase_id}' (type {control.phase_type}) has no operation '{op_name}'"
            )
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


def _success_response(request_id: str | int | None, result: Any) -> str:
    response = {
        "jsonrpc": "2.0",
        "result": result
    }
    if request_id is not None:
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
    if data is not None:
        response["error"]["data"] = data
    if request_id is not None:
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
