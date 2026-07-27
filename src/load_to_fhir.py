"""Load Synthea-generated FHIR R4 transaction bundles into the HAPI FHIR server.

Each Synthea output file is a transaction Bundle: POSTing it to the server root
inserts every resource it contains atomically (all-or-nothing). Support files
(hospital/practitioner organizations) are posted before patient bundles because
patient resources reference them.

Usage:
    python src/load_to_fhir.py [data_dir]

    data_dir defaults to data/synthea/fhir (Synthea's FHIR output folder).
"""

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "config"))
from settings import FHIR_BASE_URL

FHIR_JSON = "application/fhir+json"


def find_bundles(data_dir: Path) -> list[Path]:
    """Return bundle files, support files (hospital/practitioner) first."""
    files = sorted(data_dir.glob("*.json"))
    support = [f for f in files if f.name.startswith(("hospitalInformation", "practitionerInformation"))]
    patients = [f for f in files if f not in support]
    return support + patients


def post_bundle(path: Path) -> tuple[bool, str]:
    """POST one bundle to the FHIR server root. Returns (ok, detail)."""
    with open(path, "rb") as f:
        body = f.read()
    resp = requests.post(
        FHIR_BASE_URL,
        data=body,
        headers={"Content-Type": FHIR_JSON},
        timeout=120,  # large bundles (1-2 MB, hundreds of resources) take a while
    )
    if resp.status_code in (200, 201):
        return True, f"HTTP {resp.status_code}"
    # On failure HAPI returns an OperationOutcome explaining why; keep it short.
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


def get_count(resource: str, params: str = "") -> int:
    """Ask the server how many resources of a type it holds (_summary=count)."""
    url = f"{FHIR_BASE_URL}/{resource}?_summary=count"
    if params:
        url += f"&{params}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("total", 0)


def verify_load(expected_patients: int) -> bool:
    """Post-load validation: read counts back and compare against expectations.

    Validates against what we *expected* to load, not just > 0 — a bare
    existence check would pass even if most bundles silently failed.
    """
    patients = get_count("Patient")
    vitals = get_count("Observation", "category=vital-signs")
    print(f"\nVerification: {patients} patients (expected {expected_patients}), "
          f"{vitals} vital-sign observations.")
    if patients != expected_patients:
        print("MISMATCH: patient count differs from bundles loaded — investigate.")
        return False
    if vitals == 0:
        print("MISMATCH: no vital-sign observations found — nothing to stream later.")
        return False
    print("Verification passed.")
    return True


def main() -> int:
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/synthea/fhir")
    if not data_dir.is_dir():
        print(f"ERROR: data directory not found: {data_dir}")
        return 1

    bundles = find_bundles(data_dir)
    if not bundles:
        print(f"ERROR: no .json bundles in {data_dir} — run Synthea first")
        return 1

    print(f"Loading {len(bundles)} bundles from {data_dir} -> {FHIR_BASE_URL}")
    start = time.time()
    failures = []
    for i, path in enumerate(bundles, 1):
        ok, detail = post_bundle(path)
        status = "ok" if ok else "FAILED"
        print(f"  [{i:2}/{len(bundles)}] {path.name[:60]:60} {status} ({detail})")
        if not ok:
            failures.append(path.name)

    loaded = len(bundles) - len(failures)
    print(f"\nDone in {time.time() - start:.1f}s: loaded {loaded}/{len(bundles)} bundles.")
    if failures:
        print("Failed bundles (transaction semantics: nothing from these was stored):")
        for name in failures:
            print(f"  - {name}")

    # Post-load validation: expected patients = patient bundles that POSTed ok
    # (every bundle except the two hospital/practitioner support files).
    support_count = sum(
        1 for b in bundles
        if b.name.startswith(("hospitalInformation", "practitionerInformation"))
        and b.name not in failures
    )
    expected_patients = loaded - support_count
    ok = verify_load(expected_patients)

    return 0 if ok and not failures else 1


if __name__ == "__main__":
    sys.exit(main())
