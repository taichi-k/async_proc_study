from typing import Set

_processed: Set[str] = set()

def seen_before(job_id: str) -> bool:
    return job_id in _processed

def mark_done(job_id: str) -> None:
    _processed.add(job_id)
