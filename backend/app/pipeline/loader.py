"""
Data Loader Module
Implements the L in ETL: loading transformed records into the target data store.
Supports batch loading with checkpointing and idempotent writes.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.database import save_records

logger = logging.getLogger(__name__)


async def load_to_warehouse(
    records: List[Dict[str, Any]],
    run_id: str,
    pipeline_id: str = "",
    batch_size: int = 500,
    on_progress: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """
    Loads transformed records into the data warehouse in batches.
    Returns a summary of the load operation.
    """
    total = len(records)
    loaded = 0
    failed = 0
    batches_written = 0
    batch_num = 0

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        batch_num += 1
        try:
            await save_records(run_id, pipeline_id, batch)
            loaded += len(batch)
            batches_written += 1
        except Exception as e:
            failed += len(batch)
            logger.error("Batch %d failed for run %s: %s", batch_num, run_id, e)

        if on_progress:
            await on_progress(loaded + failed, total, f"load:batch_{batch_num}")
        await asyncio.sleep(0.03)

    if failed > 0:
        logger.warning(
            "Run %s: %d records failed to load across %d batches",
            run_id, failed, batch_num - batches_written,
        )

    return {
        "records_loaded": loaded,
        "records_failed": failed,
        "batches": batches_written,
        "load_timestamp": datetime.now(timezone.utc).isoformat(),
    }
