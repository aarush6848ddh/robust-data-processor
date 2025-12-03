"""
Pub/Sub worker Cloud Function for the Robust Data Processor pipeline.

Responsibilities:
  - Consume normalized text log messages from Pub/Sub.
  - Simulate CPU‑bound work by sleeping 0.05s per character of text
    (capped to keep within the function timeout).
  - Perform very simple PII redaction (phone‑like patterns).
  - Persist the processed log into Firestore using a **tenant‑isolated**
    and **idempotent** document key:

        tenants/{tenant_id}/processed_logs/{log_id}

Notes:
  - Tenant isolation: `tenant_id` is always part of the Firestore path,
    so data for different tenants never shares a flat collection.
  - Idempotency: `log_id` is the document ID; reprocessing the same
    message overwrites the same document instead of creating duplicates.
"""

import base64
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict

from google.cloud import firestore


_db: firestore.Client | None = None


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client()
    return _db


def _redact_sensitive(text: str) -> str:
    """
    Very simple PII redaction for demonstration only.

    Currently:
      - Redacts 7‑digit patterns like 555-0199.
      - Redacts 10‑digit patterns like 555-123-4567.
    """
    # 7-digit pattern like 555-0199
    text = re.sub(r"\b\d{3}-\d{4}\b", "[REDACTED]", text)
    # 10-digit pattern like 555-123-4567
    text = re.sub(r"\b\d{3}-\d{3}-\d{4}\b", "[REDACTED]", text)
    return text


def _simulate_heavy_processing(text: str) -> None:
    """Sleep for 0.05s per character (capped) to simulate CPU‑bound work."""
    delay_per_char = float(os.environ.get("DELAY_PER_CHAR", "0.05"))
    total_sleep = delay_per_char * len(text)
    # Cap the sleep time so we never exceed the Cloud Function timeout in extreme cases
    max_sleep = float(os.environ.get("MAX_TOTAL_SLEEP", "55"))
    time.sleep(min(total_sleep, max_sleep))


def process_log(event: Dict[str, Any], context: Any) -> None:
    """
    Pub/Sub-triggered Cloud Function worker.

    Input:
      - `event["data"]`: base64‑encoded text log (normalized by the ingest function).
      - `event["attributes"]`: should include:
          * `tenant_id` – which tenant owns this log.
          * `log_id`    – idempotent key for the processed log document.
          * `source`    – "json_upload" or "text_upload" (optional).

    Behaviour:
      1. Decode the text from base64.
      2. Validate that `tenant_id` and `log_id` are present; if not, raise so
         that Pub/Sub retries instead of writing ambiguous data.
      3. Simulate heavy CPU work via `_simulate_heavy_processing`.
      4. Redact simple phone‑like patterns.
      5. Write the result into Firestore at:

             tenants/{tenant_id}/processed_logs/{log_id}

         Using this path enforces tenant isolation and idempotency.
    """
    message_data = event.get("data", "")
    attributes = event.get("attributes") or {}

    if not message_data:
        # Nothing to do; ack and exit.
        return

    original_text = base64.b64decode(message_data).decode("utf-8")
    tenant_id = attributes.get("tenant_id")
    log_id = attributes.get("log_id")
    source = attributes.get("source", "unknown")

    if not tenant_id or not log_id:
        # Without tenant or log id we cannot safely write to Firestore in a multi-tenant way.
        # Raising an exception will cause Pub/Sub to retry / eventually DLQ depending on config.
        raise RuntimeError("Missing tenant_id or log_id in Pub/Sub message attributes")

    # Simulate heavy CPU-bound processing
    _simulate_heavy_processing(original_text)

    modified = _redact_sensitive(original_text)
    processed_at = datetime.now(timezone.utc).isoformat()

    db = _get_db()

    # Strong tenant isolation via hierarchical path:
    # tenants/{tenant_id}/processed_logs/{log_id}
    doc_ref = db.collection("tenants").document(tenant_id).collection("processed_logs").document(log_id)

    # Idempotent write: using a fixed {tenant_id, log_id} key means retries overwrite the same doc
    doc_ref.set(
        {
            "source": source,
            "original_text": original_text,
            "modified_data": modified,
            "processed_at": processed_at,
        },
        merge=True,
    )


