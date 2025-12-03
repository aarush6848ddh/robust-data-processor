"""
Unified ingestion HTTP Cloud Function for the Robust Data Processor task.

Responsibilities:
  - Expose a single public `/ingest` endpoint.
  - Accept both JSON (`application/json`) and raw text (`text/plain`) requests.
  - Normalize all inputs into a **flat text** representation.
  - Attach tenant and log metadata as Pub/Sub message attributes.
  - Publish to a Pub/Sub topic and return **202 Accepted** immediately
    (no heavy work or database access here).

This function is intentionally very small and fast so it can sustain high
request rates (1,000+ RPM). All CPU‑bound work and Firestore writes happen
in the Pub/Sub worker.
"""

import json
import os
import uuid
from typing import Any, Dict, Tuple

from google.cloud import pubsub_v1


_publisher: pubsub_v1.PublisherClient | None = None


class BadRequest(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


def _get_project_id() -> str:
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise RuntimeError("GCP_PROJECT or GOOGLE_CLOUD_PROJECT environment variable must be set")
    return project_id


def _get_topic_path() -> str:
    topic_id = os.environ.get("PUBSUB_TOPIC")
    if not topic_id:
        raise RuntimeError("PUBSUB_TOPIC environment variable must be set")
    project_id = _get_project_id()
    publisher = _get_publisher()
    return publisher.topic_path(project_id, topic_id)


def _normalize_json(request) -> Dict[str, Any]:
    """
    Normalize a JSON payload into the internal representation.

    Expected JSON body:
        {
          "tenant_id": "acme",
          "log_id": "optional-id",  # optional, UUID generated if omitted
          "text": "User 555-0199 accessed..."
        }
    """
    data = request.get_json(silent=True)
    if data is None:
        raise BadRequest("Invalid JSON body")

    tenant_id = data.get("tenant_id")
    log_id = data.get("log_id") or str(uuid.uuid4())
    text = data.get("text")

    if not tenant_id or not isinstance(tenant_id, str):
        raise BadRequest("Field 'tenant_id' is required and must be a string")
    if not text or not isinstance(text, str):
        raise BadRequest("Field 'text' is required and must be a string")

    return {
        "tenant_id": tenant_id,
        "log_id": log_id,
        "text": text,
        "source": "json_upload",
    }


def _normalize_text(request) -> Dict[str, Any]:
    """
    Normalize a raw text payload into the internal representation.

    Expected headers / body:
      - Content-Type: text/plain
      - X-Tenant-ID: <tenant_id>
      - Body: arbitrary text (e.g. log file snippet)
    """
    tenant_id = request.headers.get("X-Tenant-ID")
    if not tenant_id:
        raise BadRequest("Header 'X-Tenant-ID' is required for text/plain payloads")

    try:
        body_bytes = request.get_data(cache=False)  # type: ignore[call-arg]
    except TypeError:
        # For older function runtimes where get_data() has no cache parameter
        body_bytes = request.get_data()

    text = body_bytes.decode("utf-8")
    log_id = str(uuid.uuid4())

    return {
        "tenant_id": tenant_id,
        "log_id": log_id,
        "text": text,
        "source": "text_upload",
    }


def _publish(normalized: Dict[str, Any]) -> None:
    publisher = _get_publisher()
    topic_path = _get_topic_path()

    # Unified internal, flat text format:
    # - Message data: raw text (string) from either JSON or text upload.
    # - Attributes: minimal metadata for downstream processing
    #   (tenant isolation and idempotent keys are handled by the worker).
    data_bytes = normalized["text"].encode("utf-8")
    publisher.publish(
        topic_path,
        data=data_bytes,
        tenant_id=normalized["tenant_id"],
        log_id=normalized["log_id"],
        source=normalized["source"],
    )


def _make_response(body: Dict[str, Any], status_code: int) -> Tuple[str, int, Dict[str, str]]:
    return json.dumps(body), status_code, {"Content-Type": "application/json"}


def ingest(request):
    """
    HTTP Cloud Function entrypoint for the unified `/ingest` endpoint.

    Behaviour:
      - Only allows POST requests (returns 405 for others).
      - Dispatches based on Content-Type:
          * application/json → `_normalize_json`
          * text/plain      → `_normalize_text`
      - Publishes a normalized message to Pub/Sub using `_publish`.
      - Returns a small JSON payload with `status`, `tenant_id` and `log_id`
        with HTTP status **202 Accepted**.

    This function deliberately does not:
      - Sleep or perform CPU‑heavy work.
      - Talk to Firestore or any other database.
      - Block on Pub/Sub publish futures.

    All of that work is delegated to the Pub/Sub worker so that this
    endpoint can remain fast and resilient under high load.
    """
    try:
        if request.method != "POST":
            return _make_response({"error": "Method not allowed"}, 405)

        content_type = request.headers.get("Content-Type", "") or ""

        if "application/json" in content_type:
            normalized = _normalize_json(request)
        elif "text/plain" in content_type:
            normalized = _normalize_text(request)
        else:
            raise BadRequest("Unsupported Content-Type. Use application/json or text/plain.")

        _publish(normalized)

        return _make_response(
            {
                "status": "accepted",
                "tenant_id": normalized["tenant_id"],
                "log_id": normalized["log_id"],
            },
            202,
        )
    except BadRequest as br:
        return _make_response({"error": br.message}, br.status_code)
    except Exception as exc:  # pragma: no cover
        # In production, log to Cloud Logging for observability.
        return _make_response({"error": "Internal server error", "detail": str(exc)}, 500)


