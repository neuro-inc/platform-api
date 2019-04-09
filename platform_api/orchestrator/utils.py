import json
from datetime import timedelta

from platform_api.orchestrator.job import AggregatedRunTime


class AggregatedRunTimeJSONEncoder(json.JSONEncoder):
    """
    Converts a python object, where datetime and timedelta objects are converted
    into objects that can be decoded using the AggregatedRunTimeJSONDecoder.
    """

    def default(self, obj):
        type_str = "AggregatedRunTime"
        if isinstance(obj, AggregatedRunTime):
            gpu_time = json.dumps(
                obj.total_gpu_run_time_delta, cls=TimeDeltaJSONEncoder
            )
            non_gpu_time = json.dumps(
                obj.total_non_gpu_run_time_delta, cls=TimeDeltaJSONEncoder
            )
            return {
                "__type__": type_str,
                "total_gpu_run_time_delta": gpu_time,
                "total_non_gpu_run_time_delta": non_gpu_time,
            }
        else:
            raise ValueError(
                f"Invalid type of '{obj}': expected '{type_str}', found: '{type(obj)}'"
            )


class AggregatedRunTimeJSONDecoder(json.JSONDecoder):
    """
    Converts a json string, where datetime and timedelta objects were converted
    into objects using the AggregatedRunTimeJSONEncoder, back into a python object.
    """

    def __init__(self):
        super().__init__(object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        expected_type = "AggregatedRunTime"
        d_type = d.pop("__type__")
        if d_type == expected_type:
            gpu_time = json.loads(
                d["total_gpu_run_time_delta"], cls=TimeDeltaJSONDecoder
            )
            non_gpu_time = json.loads(
                d["total_non_gpu_run_time_delta"], cls=TimeDeltaJSONDecoder
            )
            return AggregatedRunTime(
                total_gpu_run_time_delta=gpu_time,
                total_non_gpu_run_time_delta=non_gpu_time,
            )
        else:
            raise ValueError(
                f"Invalid type of '{d}': expected '{expected_type}', found: '{type(d)}'"
            )


class TimeDeltaJSONEncoder(json.JSONEncoder):
    """
    Converts a python object, where datetime and timedelta objects are converted
    into objects that can be decoded using the TimeDeltaJSONDecoder.
    """

    def default(self, obj):
        type_str = "timedelta"
        if isinstance(obj, timedelta):
            return {
                "__type__": type_str,
                "days": obj.days,
                "seconds": obj.seconds,
                "microseconds": obj.microseconds,
            }
        else:
            raise ValueError(
                f"Invalid type of '{obj}': expected '{type_str}', found: '{type(obj)}'"
            )


class TimeDeltaJSONDecoder(json.JSONDecoder):
    """
    Converts a json string, where datetime and timedelta objects were converted
    into objects using the TimeDeltaJSONEncoder, back into a python object.
    """

    def __init__(self):
        super().__init__(object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        expected_type = "timedelta"
        d_type = d.pop("__type__")
        if d_type == expected_type:
            return timedelta(**d)
        else:
            raise ValueError(
                f"Invalid type of '{d}': expected '{expected_type}', found: '{type(d)}'"
            )
