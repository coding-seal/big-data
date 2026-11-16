"""CLI entrypoint. One subcommand per pipeline stage.

    uv run python3 src/run.py download        # bronze
    uv run python3 src/run.py build-silver    # silver
    uv run python3 src/run.py build-gold      # gold
    uv run python3 src/run.py run-question q1_topic_time

(or `just download` / `just build-silver` / `just gold` / `just question <name>`)
"""
import argparse
from pathlib import Path

from pipeline.logging import configure
from config import load_config
from pipeline import bronze, gold, silver, site
from pipeline.storage import REPO_ROOT
from questions import QUESTIONS


def main() -> None:
    configure()

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", default=str(REPO_ROOT / "config.yaml"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("download", help="fetch bronze files for the configured wiki/date range")
    sub.add_parser("build-silver", help="parse bronze into the silver tables")
    sub.add_parser("build-gold", help="run every question, write each as a Vega-Lite chart spec")
    sub.add_parser("build-site", help="render every profile's gold/*.json as a static site/ (no Spark)")
    run_q = sub.add_parser("run-question", help="run one question module against silver")
    run_q.add_argument("name", choices=sorted(QUESTIONS))

    args = parser.parse_args()
    config = load_config(Path(args.config))

    if args.command == "download":
        bronze.download(config)
    elif args.command == "build-silver":
        silver.build(config)
    elif args.command == "build-gold":
        gold.build(config)
    elif args.command == "build-site":
        site.build()
    elif args.command == "run-question":
        gold.show_question(config, args.name)


if __name__ == "__main__":
    main()
