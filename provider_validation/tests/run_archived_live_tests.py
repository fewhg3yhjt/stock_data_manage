"""Run a-stock-data's opt-in live tests with raw HTTP response capture."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import unittest
from pathlib import Path

from raw_response_archive import RawResponseArchive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--group", choices=("v39", "v310", "official"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--date", default="2026-09-18")
    args = parser.parse_args()

    repo = args.repo.resolve()
    sys.path.insert(0, str(repo / "tests"))
    if args.group == "v39":
        os.environ["ASTOCK_LIVE_V39"] = args.date
        modules = ["test_v39_sources"]
        scope = {"project": "a-stock-data", "source_commit": "f814dcfe209dd7958f4858f9d878d591ee85fb56",
                 "test_group": "V3.9 live source validation", "requested_data_date": args.date}
    elif args.group == "v310":
        os.environ["ASTOCK_LIVE_V310"] = "1"
        modules = ["test_v310_sources"]
        scope = {"project": "a-stock-data", "source_commit": "f814dcfe209dd7958f4858f9d878d591ee85fb56",
                 "test_group": "V3.10 live source validation"}
    else:
        os.environ["ASTOCK_LIVE_TRADE_DATE"] = "2026-09-04"
        os.environ["ASTOCK_LIVE_MARGIN_DATE"] = "2026-09-03"
        modules = ["test_official_data"]
        scope = {"project": "a-stock-data", "source_commit": "f814dcfe209dd7958f4858f9d878d591ee85fb56",
                 "test_group": "V3.8 official live source validation", "trade_date": "2026-09-04",
                 "margin_date": "2026-09-03"}

    loader = unittest.TestLoader()
    suite = unittest.TestSuite(loader.loadTestsFromName(name) for name in modules)
    with RawResponseArchive(args.run_id) as archive:
        with archive.scope(**scope):
            result = unittest.TextTestRunner(verbosity=2).run(suite)
    print({"tests_run": result.testsRun, "failures": len(result.failures),
           "errors": len(result.errors), "skipped": len(result.skipped),
           "archive": str(archive.directory), "finished_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()})
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
