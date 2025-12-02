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
    Very simple PII redaction for demonstration:
    - Redact patterns that look like phone numbers (e.g., 555-0199 or 555-123-4567).
    """
    # 7-digit pattern like 555-0199
    text = re.sub(r"\b\d{3}-\d{4}\b", "[REDACTED]", text)
    # 10-digit pattern like 555-123-4567
    text = re.sub(r"\b\d{3}-\d{3}-\d{4}\b", "[REDACTED]", text)
    return text


def _simulate_heavy_processing(text: str) -> None:
    # Sleep 0.05s per character
    delay_per_char = float(os.environ.get("DELAY_PER_CHAR", "0.05"))
    total_sleep = delay_per_char * len(text)
    # Cap the sleep time so we never exceed the Cloud Function timeout in extreme cases
    max_sleep = float(os.environ.get("MAX_TOTAL_SLEEP", "55"))
    time.sleep(min(total_sleep, max_sleep))


def process_log(event: Dict[str, Any], context: Any) -> None:
    """
    Pub/Sub-triggered Cloud Function worker.

    It receives normalized text logs published by the ingest function,
    simulates heavy processing, and writes the result into Firestore using
    a tenant-isolated document path:

        tenants/{tenant_id}/processed_logs/{log_id}
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


