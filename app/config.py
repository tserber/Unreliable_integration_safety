import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    lease_seconds: int = 30
    idempotency_key_ttl_hours: int = 24
    # Safety margin under the idempotency TTL: stop auto-retrying even an
    # idempotent CRM once we're this close to the key going stale, and
    # escalate to a human instead of gambling on a real duplicate.
    retry_cutoff_hours: int = 20
    crm_call_timeout_seconds: float = 5.0
    poll_interval_seconds: float = 0.5
    # How often a worker upserts its own liveness row, and how often the
    # API re-checks the fleet - kept equal by default so one worker going
    # quiet is noticed within roughly one interval.
    worker_heartbeat_interval_seconds: float = 5.0
    # A worker is considered dead once its last heartbeat is older than
    # this - comfortably more than one interval, so a single slow tick
    # doesn't flap the health check.
    worker_heartbeat_stale_seconds: float = 15.0


def load_settings(env_prefix: str = "") -> Settings:
    url = os.environ.get(
        f"{env_prefix}DATABASE_URL",
        "postgresql://handoff:handoff@localhost:5432/handoff_dev",
    )
    return Settings(database_url=url)
