"""Enforce independent line and branch coverage thresholds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _percentage(covered: int, total: int) -> float:
    return 100.0 if total == 0 else covered * 100.0 / total


def _read_totals(report_path: Path) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    totals = report.get("totals")
    if not isinstance(totals, dict):
        raise TypeError("coverage report does not contain totals")
    return totals


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--line-fail-under", type=float, required=True)
    parser.add_argument("--branch-fail-under", type=float, required=True)
    args = parser.parse_args()

    totals = _read_totals(args.report)
    covered_lines = int(totals["covered_lines"])
    total_lines = int(totals["num_statements"])
    covered_branches = int(totals["covered_branches"])
    total_branches = int(totals["num_branches"])
    line_coverage = _percentage(covered_lines, total_lines)
    branch_coverage = _percentage(covered_branches, total_branches)

    print(
        f"line coverage: {line_coverage:.2f}% "
        f"({covered_lines}/{total_lines}), required {args.line_fail_under:.2f}%"
    )
    print(
        f"branch coverage: {branch_coverage:.2f}% "
        f"({covered_branches}/{total_branches}), "
        f"required {args.branch_fail_under:.2f}%"
    )

    failed = (
        line_coverage < args.line_fail_under or branch_coverage < args.branch_fail_under
    )
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
