from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any
import datetime as dt

from mpi4py import MPI

from models.experiment_case import ExperimentCase
from src.benchmark.backends import get_backend
from src.benchmark.clock_calibration import (
    ClockCalibration,
    assess_clock_calibration,
    calibrate_mpi_clocks,
)
from src.benchmark.coordinated_drain import CoordinatedDrainState
from src.benchmark.metrics import MetricsAggregator
from src.benchmark.roles import Role, active_roles_for_scenario, assign_role


class BenchmarkController:
    """
    Rank-0 controller for one benchmark case.
    """

    def __init__(
        self,
        case: ExperimentCase,
        bootstrap_servers: str,
        output_dir: str | Path,
    ) -> None:
        self.case = case
        self.bootstrap_servers = bootstrap_servers
        self.output_dir = Path(output_dir)

        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.world_size = self.comm.Get_size()
        self.backend = get_backend(self.case.config.backend_id)
        self.backend.validate_config(self.case.config)
        self._consumer_to_close: Any | None = None

    def run(self) -> dict[str, Any]:
        """
        Run the configured benchmark case and aggregate results on rank 0.
        """
        if self.rank == 0:
            self.case.status = "running"
            self.case.ensure_output_dir()

        try:
            before_calibration: dict[int, ClockCalibration] | None = None
            after_calibration: dict[int, ClockCalibration] | None = None
            if self.case.config.latency_enabled:
                before_calibration = calibrate_mpi_clocks(
                    self.comm,
                    self.case.config.latency_clock_samples,
                )
            self.comm.Barrier()
            clock_offset_ns = (
                before_calibration[self.rank].offset_ns
                if before_calibration is not None
                else 0
            )
            if self.backend.uses_coordinated_consumer_drain(self.case.config):
                local_result = self._run_local_role_with_coordinated_drain(
                    clock_offset_ns=clock_offset_ns,
                )
            else:
                local_result = self._run_local_role(
                    clock_offset_ns=clock_offset_ns,
                )
            self._finalize_consumer_group(local_result)
            if self.case.config.latency_enabled:
                after_calibration = calibrate_mpi_clocks(
                    self.comm,
                    self.case.config.latency_clock_samples,
                )
                local_result["clock_calibration"] = assess_clock_calibration(
                    before_calibration[self.rank],
                    after_calibration[self.rank],
                    max_uncertainty_us=(
                        self.case.config.latency_clock_max_uncertainty_us
                    ),
                    max_drift_us=self.case.config.latency_clock_max_drift_us,
                )
        except Exception:
            if self.rank == 0:
                self.case.status = "failed"
            raise

        gathered_results = self.comm.gather(local_result, root=0)

        if self.rank != 0:
            return {}

        self.case.status = "completed"
        final_result = self._aggregate_gathered_results(gathered_results)
        self._write_outputs(final_result)
        return final_result

    def _run_local_role(self, clock_offset_ns: int = 0) -> dict[str, Any]:
        """
        Execute local logic for the current rank.
        """
        assignment = assign_role(self.rank, self.case.config)
        active_roles = active_roles_for_scenario(self.case.config)
        needs_consumer_readiness = self._needs_consumer_readiness(active_roles)
        is_active_role = assignment.role in active_roles
        readiness_record: dict[str, Any] | None = None
        consumer: Any | None = None

        if assignment.role == Role.CONSUMER and is_active_role:
            consumer = self.backend.create_consumer_worker(
                config=self.case.config,
                rank=self.rank,
                bootstrap_servers=self.bootstrap_servers,
                case_id=self.case.case_id,
                clock_offset_ns=clock_offset_ns,
            )
        elif assignment.role == Role.PRODUCER and is_active_role:
            producer = self.backend.create_producer_worker(
                config=self.case.config,
                rank=self.rank,
                bootstrap_servers=self.bootstrap_servers,
                producer_index=assignment.producer_index or 0,
                clock_offset_ns=clock_offset_ns,
            )

        if needs_consumer_readiness:
            readiness_record = self._coordinate_consumer_readiness(
                role=assignment.role,
                is_active_role=is_active_role,
                consumer=consumer,
            )

        if assignment.role == Role.CONTROLLER:
            return {
                "rank": self.rank,
                "role": assignment.role.value,
                "active": True,
                "phase": self.case.config.scenario,
                "readiness": readiness_record,
                "metrics": {},
            }

        if not is_active_role:
            return self._build_inactive_role_result(assignment.role, readiness_record)

        if assignment.role == Role.PRODUCER:
            producer = self.backend.create_producer_worker(
                config=self.case.config,
                rank=self.rank,
                bootstrap_servers=self.bootstrap_servers,
                producer_index=assignment.producer_index or 0,
                clock_offset_ns=clock_offset_ns,
            )
            metrics = producer.run()
            return {
                "rank": self.rank,
                "role": assignment.role.value,
                "active": True,
                "phase": self.case.config.scenario,
                "readiness": readiness_record,
                "metrics": metrics.to_dict(),
            }

        if assignment.role == Role.CONSUMER:
            if consumer is None:
                consumer = self.backend.create_consumer_worker(
                    config=self.case.config,
                    rank=self.rank,
                    bootstrap_servers=self.bootstrap_servers,
                    case_id=self.case.case_id,
                    clock_offset_ns=clock_offset_ns,
                )

            if needs_consumer_readiness:
                metrics = consumer.run(prepared=True, close_after_run=False)
            else:
                metrics = consumer.run(close_after_run=False)
            self._consumer_to_close = consumer

            return {
                "rank": self.rank,
                "role": assignment.role.value,
                "active": True,
                "phase": self.case.config.scenario,
                "readiness": readiness_record,
                "metrics": metrics.to_dict(),
            }

        raise ValueError(f"Unsupported role assignment for rank {self.rank}")

    def _run_local_role_with_coordinated_drain(
        self,
        clock_offset_ns: int = 0,
    ) -> dict[str, Any]:
        """Run a simultaneous case with drain starting after producer flush."""
        assignment = assign_role(self.rank, self.case.config)
        active_roles = active_roles_for_scenario(self.case.config)
        is_active_role = assignment.role in active_roles
        consumer: Any | None = None
        producer: Any | None = None

        if assignment.role == Role.CONSUMER and is_active_role:
            consumer = self.backend.create_consumer_worker(
                config=self.case.config,
                rank=self.rank,
                bootstrap_servers=self.bootstrap_servers,
                case_id=self.case.case_id,
                clock_offset_ns=clock_offset_ns,
            )
        elif assignment.role == Role.PRODUCER and is_active_role:
            producer = self.backend.create_producer_worker(
                config=self.case.config,
                rank=self.rank,
                bootstrap_servers=self.bootstrap_servers,
                producer_index=assignment.producer_index or 0,
                clock_offset_ns=clock_offset_ns,
            )

        readiness_record = self._coordinate_consumer_readiness(
            role=assignment.role,
            is_active_role=is_active_role,
            consumer=consumer,
        )

        if assignment.role == Role.PRODUCER and is_active_role:
            if producer is None:
                raise RuntimeError("Producer rank has no backend worker")
            producer.run_send_phase()
            producer.flush_and_close()
        elif assignment.role == Role.CONSUMER and is_active_role:
            if consumer is None:
                raise RuntimeError("Consumer rank has no backend worker")
            consumer.run_measurement_phase(prepared=True)
            self._consumer_to_close = consumer

        # Producers enter this collective only after all delivery callbacks and
        # flush accounting have completed. Consumers keep polling while they
        # wait, so producer-side queue drain cannot consume the final consumer
        # drain budget.
        flush_complete = self.comm.Ibarrier()
        if consumer is not None and is_active_role:
            while not flush_complete.Test():
                consumer.poll_during_producer_flush(max_duration_sec=0.1)
        else:
            flush_complete.Wait()

        local_delivered = (
            int(producer.metrics.messages_delivered)
            if producer is not None
            else 0
        )
        local_consumed = (
            int(consumer.total_measurement_records)
            if consumer is not None
            else 0
        )
        total_delivered = int(
            self.comm.allreduce(local_delivered, op=MPI.SUM)
        )
        total_consumed = int(
            self.comm.allreduce(local_consumed, op=MPI.SUM)
        )

        if consumer is not None and is_active_role:
            consumer.begin_post_flush_drain(
                delivered_target_records=total_delivered,
                coordinated=True,
            )

        drain_state = CoordinatedDrainState(
            delivered_target_records=total_delivered,
            timeout_sec=self.case.config.drain_timeout_sec,
            step_sec=0.25,
            consumed_records=total_consumed,
        )

        while drain_state.should_continue:
            if consumer is not None and is_active_role:
                consumer.drain_step(max_duration_sec=drain_state.step_sec)
                local_consumed = int(consumer.total_measurement_records)
            else:
                time.sleep(drain_state.step_sec)
                local_consumed = 0
            total_consumed = int(
                self.comm.allreduce(local_consumed, op=MPI.SUM)
            )
            drain_state.record_step(total_consumed)

        if consumer is not None and is_active_role:
            consumer.complete_post_flush_drain(
                reason=drain_state.completion_reason,
                globally_consumed_records=total_consumed,
            )

        metrics: dict[str, Any] = {}
        if producer is not None:
            metrics = producer.metrics.to_dict()
        elif consumer is not None:
            metrics = consumer.metrics.to_dict()

        return {
            "rank": self.rank,
            "role": assignment.role.value,
            "active": (
                True
                if assignment.role == Role.CONTROLLER
                else is_active_role
            ),
            "phase": self.case.config.scenario,
            "readiness": readiness_record,
            "metrics": metrics,
            "coordinated_drain": {
                "enabled": True,
                "producer_flush_boundary": "all_producer_ranks_completed_flush",
                "delivered_target_records": total_delivered,
                "consumed_records_at_completion": total_consumed,
                "configured_post_flush_timeout_sec": (
                    self.case.config.drain_timeout_sec
                ),
                "step_sec": drain_state.step_sec,
                "steps_completed": drain_state.steps_completed,
                "completion_reason": drain_state.completion_reason,
                "complete": drain_state.complete,
            },
        }

    def _finalize_consumer_group(self, local_result: dict[str, Any]) -> None:
        """
        Keep every consumer in the group until all ranks finish their run loop.

        Closing consumers independently can trigger a partition reassignment
        while slower ranks are still draining. The resulting replay is real
        consumer-group behavior, but it would contaminate per-case duplicate
        and ordering checks. A barrier followed by an all-rank close-status
        exchange makes the drain boundary deterministic.
        """
        self.comm.Barrier()
        close_error = ""
        if self._consumer_to_close is not None:
            try:
                self._consumer_to_close.close()
                local_result["metrics"] = (
                    self._consumer_to_close.metrics.to_dict()
                )
            except Exception as exc:
                close_error = f"{type(exc).__name__}: {exc}"

        close_results = self.comm.allgather(
            {
                "rank": self.rank,
                "error": close_error,
            }
        )
        failures = [
            record
            for record in close_results
            if record.get("error")
        ]
        if failures:
            details = "; ".join(
                f"rank {record['rank']}: {record['error']}"
                for record in failures
            )
            raise RuntimeError(
                "Consumer close failed after synchronized drain: " + details
            )

    @staticmethod
    def _needs_consumer_readiness(active_roles: set[Role]) -> bool:
        """
        Return True for scenarios where producers and consumers run together.
        """
        return Role.PRODUCER in active_roles and Role.CONSUMER in active_roles

    def _coordinate_consumer_readiness(
        self,
        role: Role,
        is_active_role: bool,
        consumer: Any | None,
    ) -> dict[str, Any]:
        """
        Prepare consumers and share readiness status across all MPI ranks.

        A plain barrier can deadlock if one consumer fails before reaching it.
        allgather lets every rank report success or failure first, then all ranks
        either start together or fail together with a useful error message.
        """
        local_record: dict[str, Any] = {
            "rank": self.rank,
            "role": role.value,
            "consumer_rank": role == Role.CONSUMER and is_active_role,
            "ready": True,
        }

        if role == Role.CONSUMER and is_active_role:
            try:
                if consumer is None:
                    raise RuntimeError("Consumer rank has no ConsumerWorker")
                consumer.prepare()
                metrics = consumer.metrics.to_dict()
                local_record.update(
                    {
                        "ready": True,
                        "assignment_received": metrics.get(
                            "readiness_assignment_received",
                            False,
                        ),
                        "assignment_count": metrics.get(
                            "readiness_assignment_count",
                            0,
                        ),
                        "elapsed_sec": metrics.get("readiness_elapsed_sec", 0.0),
                        "note": metrics.get("readiness_note", ""),
                    }
                )
            except Exception as exc:
                local_record.update(
                    {
                        "ready": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        local_record["close_error"] = "failed to close consumer"

        readiness_results = self.comm.allgather(local_record)
        failed_records = [
            record
            for record in readiness_results
            if record.get("consumer_rank") and not record.get("ready")
        ]

        if failed_records:
            details = "; ".join(
                f"rank {record.get('rank')}: {record.get('error', 'not ready')}"
                for record in failed_records
            )
            if self.rank == 0:
                self.case.status = "failed"
            raise RuntimeError(
                "Consumer readiness failed before benchmark start: " + details
            )

        return local_record

    def _build_inactive_role_result(
        self,
        role: Role,
        readiness_record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Return a per-rank record for roles that are configured but inactive.
        """
        return {
            "rank": self.rank,
            "role": role.value,
            "active": False,
            "phase": self.case.config.scenario,
            "readiness": readiness_record,
            "metrics": {},
            "notes": [
                f"{role.value} rank is idle for scenario {self.case.config.scenario}",
            ],
        }

    def _aggregate_gathered_results(
        self,
        gathered_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Aggregate all gathered rank results into one case result.
        """
        per_rank_results: list[dict[str, Any]] = list(gathered_results)

        producer_metrics_list = [
            record["metrics"]
            for record in gathered_results
            if record.get("role") == Role.PRODUCER.value
            and record.get("active") is not False
        ]
        consumer_metrics_list = [
            record["metrics"]
            for record in gathered_results
            if record.get("role") == Role.CONSUMER.value
            and record.get("active") is not False
        ]

        aggregator = MetricsAggregator()
        aggregated_metrics = aggregator.aggregate_from_dicts(
            producer_metrics_list=producer_metrics_list,
            consumer_metrics_list=consumer_metrics_list,
        )

        result: dict[str, Any] = {
            "case": self.case.to_dict(),
            "config": self.case.config.to_dict(),
            "world_size": self.world_size,
            "scenario_execution": {
                "scenario": self.case.config.scenario,
                "active_roles": [
                    role.value for role in sorted(
                        active_roles_for_scenario(self.case.config),
                        key=lambda item: item.value,
                    )
                ],
            },
            "per_rank_results": per_rank_results,
            "aggregated_metrics": aggregated_metrics,
            "timeline": self._build_timeline(aggregated_metrics),
        }
        coordinated_records = [
            record.get("coordinated_drain")
            for record in per_rank_results
            if isinstance(record.get("coordinated_drain"), dict)
        ]
        if coordinated_records:
            result["coordinated_drain"] = coordinated_records[0]
        result["latency_validation"] = self._build_latency_validation(
            per_rank_results,
            aggregated_metrics,
        )
        return result

    def _build_latency_validation(
        self,
        per_rank_results: list[dict[str, Any]],
        aggregated_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.case.config.latency_enabled:
            return {
                "enabled": False,
                "valid": False,
                "reason": "end-to-end latency instrumentation disabled",
            }

        active_records = [
            record
            for record in per_rank_results
            if record.get("active") is not False
        ]
        calibration_records = [
            record.get("clock_calibration")
            for record in active_records
            if isinstance(record.get("clock_calibration"), dict)
        ]
        clock_valid = (
            len(calibration_records) == len(active_records)
            and all(bool(record.get("valid")) for record in calibration_records)
        )
        consumers = aggregated_metrics.get("consumers", {})
        if not isinstance(consumers, dict):
            consumers = {}
        histogram = consumers.get("latency_histogram", {})
        if not isinstance(histogram, dict):
            histogram = {}
        producers = aggregated_metrics.get("producers", {})
        if not isinstance(producers, dict):
            producers = {}
        sample_count = int(histogram.get("count", 0))
        delivered_sample_count = int(
            producers.get("latency_samples_delivered", 0)
        )
        negative_count = int(histogram.get("negative_count", 0))
        envelope_valid = int(consumers.get("invalid_envelope_count", 0)) == 0
        sample_completeness_valid = (
            delivered_sample_count > 0
            and sample_count == delivered_sample_count
        )
        valid = (
            clock_valid
            and envelope_valid
            and sample_completeness_valid
            and negative_count == 0
        )

        return {
            "enabled": True,
            "valid": valid,
            "clock_valid": clock_valid,
            "envelope_valid": envelope_valid,
            "sample_count": sample_count,
            "delivered_sample_count": delivered_sample_count,
            "sample_completeness_valid": sample_completeness_valid,
            "negative_latency_count": negative_count,
            "sample_every": self.case.config.latency_sample_every,
            "clock_calibrations": calibration_records,
            "reason": (
                "valid"
                if valid
                else (
                    "latency invalid: clock limits, envelope integrity, exact "
                    "sample completeness, or negative-latency checks failed"
                )
            ),
        }

    def _build_timeline(self, aggregated_metrics: dict[str, Any]) -> dict[str, Any]:
        producers = aggregated_metrics.get("producers", {})
        consumers = aggregated_metrics.get("consumers", {})
        if not isinstance(producers, dict):
            producers = {}
        if not isinstance(consumers, dict):
            consumers = {}

        events: list[dict[str, Any]] = []
        self._append_timeline_event(
            events,
            "producer_start",
            "Producer ranks start sending",
            producers.get("first_start_time_unix"),
            "mpi",
        )
        self._append_timeline_event(
            events,
            "consumer_start",
            "Consumer ranks start polling",
            consumers.get("first_start_time_unix"),
            "mpi",
        )
        pressure_candidates = [
            float(value or 0.0)
            for value in (
                producers.get("first_start_time_unix"),
                consumers.get("first_start_time_unix"),
            )
            if float(value or 0.0) > 0
        ]
        if pressure_candidates:
            pressure_label = (
                "Kafka load pressure starts"
                if self.backend.backend_id == "kafka"
                else f"{self.backend.backend_id.capitalize()} load pressure starts"
            )
            self._append_timeline_event(
                events,
                "load_pressure_start",
                pressure_label,
                min(pressure_candidates),
                "derived",
            )
        self._append_timeline_event(
            events,
            "producer_send_loop_end",
            "Producer send window ends",
            producers.get("last_send_loop_end_time_unix"),
            "mpi",
        )
        self._append_timeline_event(
            events,
            "producer_flush_complete",
            "All producer delivery accounting completes",
            producers.get("last_end_time_unix"),
            "mpi",
        )
        self._append_timeline_event(
            events,
            "post_flush_drain_start",
            "Coordinated post-flush consumer drain starts",
            consumers.get("first_post_flush_drain_start_time_unix"),
            "mpi",
        )
        self._append_timeline_event(
            events,
            "post_flush_drain_end",
            "Coordinated post-flush consumer drain ends",
            consumers.get("last_post_flush_drain_end_time_unix"),
            "mpi",
        )
        workload_end_candidates = [
            float(value or 0.0)
            for value in (
                producers.get("last_end_time_unix"),
                consumers.get("last_end_time_unix"),
            )
            if float(value or 0.0) > 0
        ]
        if workload_end_candidates:
            self._append_timeline_event(
                events,
                "workload_end",
                "Producer/consumer workload ends",
                max(workload_end_candidates),
                "derived",
            )

        return {
            "format": "benchmark_timeline.v1",
            "events": sorted(events, key=lambda item: float(item.get("unix", 0.0))),
        }

    @staticmethod
    def _append_timeline_event(
        events: list[dict[str, Any]],
        name: str,
        label: str,
        timestamp: Any,
        source: str,
    ) -> None:
        try:
            unix_time = float(timestamp)
        except (TypeError, ValueError):
            return
        if unix_time <= 0:
            return
        events.append(
            {
                "name": name,
                "label": label,
                "unix": unix_time,
                "iso": dt.datetime.fromtimestamp(
                    unix_time,
                    tz=dt.timezone.utc,
                ).isoformat(),
                "source": source,
            }
        )

    def _write_outputs(self, final_result: dict[str, Any]) -> None:
        """
        Write core case outputs to disk.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        data_dir = self.output_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        config_snapshot = self.case.config.to_dict()
        outputs = (
            (self.output_dir / "case_config_snapshot.json", config_snapshot),
            (data_dir / "case_config_snapshot.json", config_snapshot),
            (self.output_dir / "benchmark_result.json", final_result),
            (data_dir / "benchmark_result.json", final_result),
        )

        for output_path, payload in outputs:
            with output_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
