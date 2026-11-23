"""Gold stage: question results as Vega-Lite chart specs.

Data and encoding come from the question module; the mark type comes from
config.yaml's `gold:` section if set, else the module's default_mark() --
so changing a chart's type is a config edit, not a code change.
"""
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from pipeline.charts import to_vega_lite
from pipeline.spark import get_spark
from config import Config
from questions import QUESTIONS
from questions.pages import ARTICLE_HOURS, ARTICLE_HOUR_INPUTS, article_hours

from . import silver
from .storage import gold_dir

log = logging.getLogger(__name__)


@contextmanager
def _spark(app_name: str) -> Iterator[SparkSession]:
    spark = get_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()


def build(config: Config, question_names: list[str] | None = None) -> None:
    questions = {name: QUESTIONS[name] for name in (question_names or QUESTIONS)}
    consumers = {
        name for name, module in questions.items()
        if getattr(module, "USES_ARTICLE_HOURS", False)
    }
    if len(consumers) == 1:
        consumers.clear()
    gold = gold_dir(config)
    gold.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix=".build-gold-", dir=gold) as workdir:
        work = Path(workdir)
        hourly_path = work / ARTICLE_HOURS
        for name, module in questions.items():
            reuse_hourly = name in consumers
            if reuse_hourly and not hourly_path.exists():
                log.info("gold: preparing shared article-hour views")
                with _spark("build-gold: article hours") as spark:
                    inputs = silver.read_tables(spark, ARTICLE_HOUR_INPUTS, config)
                    article_hours(inputs).write.parquet(str(hourly_path))

            log.info("gold: starting %s", name)
            with _spark(f"build-gold: {name}") as spark:
                names = module.required_tables()
                if reuse_hourly:
                    names = [
                        table for table in names
                        if table not in {"pageviews_fact", "redirect_dim"}
                    ]
                tables = silver.read_tables(spark, names, config)
                if reuse_hourly:
                    tables[ARTICLE_HOURS] = spark.read.parquet(str(hourly_path))
                rows = [row.asDict() for row in module.run(tables).collect()]

            mark = config.gold.get(name, module.default_mark())
            spec = to_vega_lite(rows, module.chart_encoding(), mark=mark, title=name)
            out_path = gold / f"{name}.json"
            pending = work / out_path.name
            pending.write_text(json.dumps(spec, indent=2), encoding="utf-8")
            pending.replace(out_path)
            log.info("gold: %s -> %s (%d rows, mark=%s)", name, out_path, len(rows), mark)
            if reuse_hourly:
                consumers.remove(name)
                if not consumers:
                    rmtree(hourly_path)


def show_question(config: Config, name: str) -> None:
    """One question's result to stdout, for iterating without writing gold."""
    module = QUESTIONS[name]
    with _spark(f"question-{name}") as spark:
        tables = silver.read_tables(spark, module.required_tables(), config)
        module.run(tables).show(20, truncate=False)
