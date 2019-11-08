from datetime import timedelta

from platform_api.handlers.stats_handler import (
    convert_run_time_to_response,
    timedelta_to_minutes,
)
from platform_api.orchestrator.job import AggregatedRunTime


def test_to_primitive() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_time_minutes": 30,
        "total_non_gpu_run_time_minutes": 60,
    }


def test_to_primitive_gpu_not_defined() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta.max,
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_non_gpu_run_time_minutes": 60
    }


def test_to_primitive_non_gpu_not_defined() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta.max,
    )
    assert convert_run_time_to_response(run_time) == {"total_gpu_run_time_minutes": 30}


def test_to_primitive_gpu_zero() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=0),
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_time_minutes": 0,
        "total_non_gpu_run_time_minutes": 60,
    }


def test_to_primitive_non_gpu_zero() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta(minutes=0),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_time_minutes": 30,
        "total_non_gpu_run_time_minutes": 0,
    }


def test__time_delta_to_minutes_millliseconds_less_than_half() -> None:
    delta = timedelta(minutes=0, seconds=10, milliseconds=29)
    assert timedelta_to_minutes(delta) == 0


def test_timedelta_to_minutes_millliseconds_equals_to_half() -> None:
    delta = timedelta(minutes=0, seconds=10, milliseconds=30)
    assert timedelta_to_minutes(delta) == 0


def test_timedelta_to_minutes_millliseconds_greater_then_half() -> None:
    delta = timedelta(minutes=0, seconds=10, milliseconds=31)
    assert timedelta_to_minutes(delta) == 0


def test_timedelta_to_minutes_seconds_less_than_half() -> None:
    delta = timedelta(minutes=0, seconds=29)
    assert timedelta_to_minutes(delta) == 0


def test_timedelta_to_minutes_seconds_equals_to_half() -> None:
    delta = timedelta(minutes=0, seconds=30)
    assert timedelta_to_minutes(delta) == 0


def test_timedelta_to_minutes_seconds_greater_then_half() -> None:
    delta = timedelta(minutes=0, seconds=31)
    assert timedelta_to_minutes(delta) == 1


def test_timedelta_to_minutes_minutes_non_zero() -> None:
    delta = timedelta(minutes=10, seconds=15)
    assert timedelta_to_minutes(delta) == 10


def test_timedelta_to_minutes_max() -> None:
    delta = timedelta.max
    assert timedelta_to_minutes(delta) is None
