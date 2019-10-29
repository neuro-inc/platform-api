from datetime import timedelta

from platform_api.handlers.stats_handler import convert_run_time_to_response
from platform_api.orchestrator.job import AggregatedRunTime


def test_to_primitive() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_minutes": 30,
        "total_non_gpu_run_minutes": 60,
    }


def test_to_primitive_gpu_not_defined() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta.max,
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {"total_non_gpu_run_minutes": 60}


def test_to_primitive_non_gpu_not_defined() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta.max,
    )
    assert convert_run_time_to_response(run_time) == {"total_gpu_run_minutes": 30}


def test_to_primitive_gpu_zero() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=0),
        total_non_gpu_run_time_delta=timedelta(minutes=60),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_minutes": 0,
        "total_non_gpu_run_minutes": 60,
    }


def test_to_primitive_non_gpu_zero() -> None:
    run_time = AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=30),
        total_non_gpu_run_time_delta=timedelta(minutes=0),
    )
    assert convert_run_time_to_response(run_time) == {
        "total_gpu_run_minutes": 30,
        "total_non_gpu_run_minutes": 0,
    }
