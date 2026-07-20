#!/usr/bin/env python3
"""Generate random domain-event payloads with Kafka-style headers for header-router smoke tests.

Stdout: one JSON object per line (compact).
With --print-kcat: also print a ready-to-run kcat produce command on stderr.
With --produce: pipe each payload to kcat (requires kcat on PATH).
"""

from __future__ import annotations

import argparse
import json
import random
import string
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

DOMAIN_TYPES = (
    "programpayment-entity",
    "associated-provider",
    "service-delivery",
    "associated-person",
    "user-role",
)

SOURCE_SYSTEMS = (
    "SF_PRV2_2",
    "EVENTXCHANGE",
    "SF_PRV2_1",
    "MULE_ADAPTER",
)

USER_ROLES = (
    "Provider Staff (Registered Provider)",
    "Provider Admin",
    "Care Coordinator",
    "Billing Specialist",
)

FIRST_NAMES = ("Alex", "Jordan", "Sam", "Riley", "Casey", "Morgan", "Taylor")
LAST_NAMES = ("Nguyen", "Patel", "Smith", "Garcia", "Kim", "Brown", "Lee")
SERVICE_STATUSES = ("SCHEDULED", "IN_PROGRESS", "COMPLETED", "CANCELLED")
PAYMENT_STATUSES = ("PENDING", "APPROVED", "PAID", "REJECTED")
CURRENCIES = ("AUD", "USD", "NZD")


def _sf_id(prefix: str) -> str:
    """Salesforce-style 18-char Id: 3-char prefix + 15 alphanumeric."""
    alphabet = string.ascii_letters + string.digits
    return prefix + "".join(random.choices(alphabet, k=15))


def _iso_now(offset_seconds: int = 0) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _iso_date(offset_days: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=offset_days)).strftime("%Y-%m-%d")


def _maybe_null(value: Any, null_chance: float = 0.25) -> Any:
    return None if random.random() < null_chance else value


def build_data(domain_type: str) -> dict[str, Any]:
    last_mod = _iso_now(-random.randint(0, 3600))
    if domain_type == "user-role":
        return {
            "IsActive": random.choice([True, False]),
            "ACRId": _sf_id("07k"),
            "AccountId": _sf_id("001"),
            "ContactId": _sf_id("003"),
            "Roles": random.choice(USER_ROLES),
            "StartDate": _iso_date(-random.randint(0, 30)),
            "EndDate": _maybe_null(_iso_date(random.randint(30, 365))),
            "LastModifiedDate": last_mod,
        }
    if domain_type == "associated-provider":
        return {
            "ProviderId": _sf_id("001"),
            "NPI": "".join(random.choices(string.digits, k=10)),
            "IsActive": random.choice([True, False]),
            "LastModifiedDate": last_mod,
        }
    if domain_type == "associated-person":
        return {
            "PersonId": _sf_id("003"),
            "FirstName": random.choice(FIRST_NAMES),
            "LastName": random.choice(LAST_NAMES),
            "IsActive": random.choice([True, False]),
        }
    if domain_type == "service-delivery":
        return {
            "ServiceId": _sf_id("a0S"),
            "DeliveryDate": _iso_date(-random.randint(0, 14)),
            "Status": random.choice(SERVICE_STATUSES),
        }
    if domain_type == "programpayment-entity":
        return {
            "PaymentId": _sf_id("a1P"),
            "Amount": round(random.uniform(10.0, 9999.99), 2),
            "Currency": random.choice(CURRENCIES),
            "Status": random.choice(PAYMENT_STATUSES),
        }
    raise ValueError(f"unknown domainType: {domain_type}")


def generate_event(
    domain_type: str | None = None,
    source_system: str | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    domain = domain_type or random.choice(DOMAIN_TYPES)
    if domain not in DOMAIN_TYPES:
        raise ValueError(f"domainType must be one of {DOMAIN_TYPES}, got {domain!r}")

    correlation_id = str(uuid.uuid4())
    transaction_id = str(uuid.uuid4())
    src = source_system or random.choice(SOURCE_SYSTEMS)
    domain_record_id = _sf_id("07k")
    timestamp = _iso_now()

    headers = {
        "correlationId": correlation_id,
        "transactionId": transaction_id,
        "domainType": domain,
        "sourceSystem": src,
    }

    payload = {
        "_meta": {
            "transaction_metadata": {
                "transaction_id": transaction_id,
                "timestamp": timestamp,
                "sub_domain_record_id": None,
                "sub_domain_key": None,
                "source_system": random.choice(("EVENTXCHANGE", src)),
                "source_entity_id": domain_record_id,
                "last_modified_date_time": _maybe_null(_iso_now(-60)),
                "domain_record_id": domain_record_id,
                "domain_key": domain,
                "created_date_time": _maybe_null(_iso_now(-86400)),
                "correlation_id": correlation_id,
            }
        },
        "data": build_data(domain),
    }
    return headers, payload


def produce_with_kcat(
    headers: dict[str, str],
    payload_json: str,
    bootstrap: str,
    topic: str,
) -> None:
    cmd = [
        "kcat",
        "-b",
        bootstrap,
        "-t",
        topic,
        "-P",
    ]
    for k, v in headers.items():
        cmd.extend(["-H", f"{k}={v}"])
    try:
        subprocess.run(cmd, input=payload_json.encode("utf-8"), check=True)
    except FileNotFoundError:
        print(
            "error: kcat not found on PATH; install kafkacat/kcat or use --print-kcat",
            file=sys.stderr,
        )
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"error: kcat failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode or 1)


def format_kcat_command(
    headers: dict[str, str],
    payload_json: str,
    bootstrap: str,
    topic: str,
) -> str:
    header_args = " ".join(f"-H {k}={v}" for k, v in headers.items())
    escaped = payload_json.replace("'", "'\\''")
    return f"echo '{escaped}' | kcat -b {bootstrap} -t {topic} -P {header_args}"


def produce_in_cluster(
    headers: dict[str, str],
    payload_json: str,
    bootstrap: str,
    topic: str,
    namespace: str,
    kcat_image: str,
) -> None:
    """Produce via a short-lived kcat pod (works with Kind in-cluster listeners)."""
    import base64

    b64 = base64.b64encode(payload_json.encode("utf-8")).decode("ascii")
    header_args = " ".join(f"-H {k}={v}" for k, v in headers.items())
    shell_cmd = f"echo {b64} | base64 -d | kcat -b {bootstrap} -t {topic} -P {header_args}"
    pod_name = "kcat-produce-" + headers.get("domainType", "msg").replace("-", "")[:20]
    try:
        subprocess.run(
            [
                "kubectl",
                "run",
                pod_name,
                "-n",
                namespace,
                "--rm",
                "-i",
                "--restart=Never",
                f"--image={kcat_image}",
                "--command",
                "--",
                "sh",
                "-c",
                shell_cmd,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"error: in-cluster produce failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode or 1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate random domain-event payloads for header-router smoke tests."
    )
    parser.add_argument(
        "--domain-type",
        choices=DOMAIN_TYPES,
        help="Fixed domainType (default: random per record)",
    )
    parser.add_argument(
        "--source-system",
        help="Fixed sourceSystem header (default: random)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of payloads to generate (default: 1)",
    )
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap for kcat (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="Ingest",
        help="Kafka topic for kcat (default: Ingest)",
    )
    parser.add_argument(
        "--print-kcat",
        action="store_true",
        help="Print a kcat produce command on stderr for each payload",
    )
    parser.add_argument(
        "--produce",
        action="store_true",
        help="Produce each payload via kcat on the host (requires kcat on PATH)",
    )
    parser.add_argument(
        "--produce-in-cluster",
        action="store_true",
        help="Produce via a short-lived kcat pod in Kubernetes (Kind-friendly)",
    )
    parser.add_argument(
        "--k8s-namespace",
        default="kafka",
        help="Namespace for in-cluster kcat pod (default: kafka)",
    )
    parser.add_argument(
        "--kcat-image",
        default="edenhill/kcat:1.7.1",
        help="kcat image for --produce-in-cluster (default: edenhill/kcat:1.7.1)",
    )
    parser.add_argument(
        "--in-cluster-bootstrap",
        default="kind-kafka-kafka-bootstrap:9092",
        help="Bootstrap for --produce-in-cluster (default: kind-kafka-kafka-bootstrap:9092)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON instead of compact single-line",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be >= 1")

    for _ in range(args.count):
        headers, payload = generate_event(
            domain_type=args.domain_type,
            source_system=args.source_system,
        )
        if args.pretty:
            payload_json = json.dumps(payload, indent=2)
        else:
            payload_json = json.dumps(payload, separators=(",", ":"))

        print(payload_json)

        if args.print_kcat:
            print(
                format_kcat_command(headers, payload_json, args.bootstrap, args.topic),
                file=sys.stderr,
            )

        if args.produce:
            produce_with_kcat(headers, payload_json, args.bootstrap, args.topic)

        if args.produce_in_cluster:
            produce_in_cluster(
                headers,
                payload_json,
                args.in_cluster_bootstrap,
                args.topic,
                args.k8s_namespace,
                args.kcat_image,
            )


if __name__ == "__main__":
    main()
