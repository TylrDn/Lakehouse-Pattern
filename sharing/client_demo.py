"""Delta Sharing client demo — read the share as a downstream recipient.

Run *after* the OSS Delta Sharing server is up (see README). This script
uses the ``delta-sharing`` Python client which is the same code a Tableau /
Power BI / Excel connector uses under the hood.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile", default=str(Path(__file__).with_name("recipient.share"))
    )
    parser.add_argument("--table", default="lakehouse_pattern_public.gold.daily_revenue")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    import delta_sharing

    df = delta_sharing.load_as_pandas(f"{args.profile}#{args.table}", limit=args.limit)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
