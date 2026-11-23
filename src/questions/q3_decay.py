"""
Q3: how fast does traffic decay after a page's peak hour.

The decay maths is pure window/groupBy work over pageviews_fact, but it has
to run on *articles*, not on URL titles: a redirect carries its own hourly
series in pageviews_fact, so keying on page_title would chart
`Hurricane_Melissa` and a redirect pointing at it as two unrelated curves,
each peaking lower than the article actually did. page_dim/redirect_dim are
required for that fold alone (questions/pages.py) -- no new source.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from questions.pages import ARTICLE_HOUR_INPUTS, article_hours

# Same reasoning as q1's TOP_N_CATEGORIES: ungated this is one row per
# (page, hour) for every page that ever appears, most of it one-off noise.
# Capping to the top-N pages by peak/avg ratio keeps the decay curve readable.
TOP_N_PAGES = 10
USES_ARTICLE_HOURS = True


def required_tables() -> list[str]:
    return list(ARTICLE_HOUR_INPUTS)


def chart_encoding() -> dict:
    return {
        "x": {"field": "hours_since_peak", "type": "quantitative", "title": "Hours since peak"},
        "y": {"field": "pct_of_peak", "type": "quantitative", "title": "% of peak views"},
        "color": {"field": "page_title", "type": "nominal"},
        "tooltip": [
            {"field": "page_title"}, {"field": "hours_since_peak"},
            {"field": "views"}, {"field": "peak_views"},
        ],
    }


def default_mark() -> str:
    return "line"


def run(tables: dict[str, DataFrame]) -> DataFrame:
    page_dim = tables["page_dim"]
    ts = article_hours(tables)

    # Sort by descending views and ascending time to keep the earliest peak.
    peak_order = F.when(F.col("views").isNotNull(), F.struct(-F.col("views"), "ts"))
    peaks = ts.groupBy("page_id").agg(
        F.max("views").alias("peak_views"),
        F.avg("views").alias("avg_views"),
        F.min_by("ts", peak_order).alias("peak_ts"),
    )

    # Rank by peak/avg, not absolute peak views -- a page with 30M views/hour
    # every hour has a bigger peak than a real spike but no decay to chart
    # (Main_Page: 1.52x its own baseline, a flat diurnal curve). Same
    # reasoning as q2's SPIKE_RATIO.
    top_pages = (
        peaks.filter(F.col("avg_views") > 0)
        .withColumn("peak_ratio", F.col("peak_views") / F.col("avg_views"))
        .orderBy(F.desc("peak_ratio"), "page_id")
        .limit(TOP_N_PAGES)
        .select("page_id", "peak_views", "peak_ts")
    )

    decay = (
        ts.join(F.broadcast(top_pages), "page_id")
        .withColumn(
            "hours_since_peak",
            (F.col("ts").cast("long") - F.col("peak_ts").cast("long")) / 3600,
        )
        .filter(F.col("hours_since_peak") >= 0)
        .withColumn("pct_of_peak", F.col("views") / F.col("peak_views") * 100)
    )

    # page_dim holds the canonical title; the chart colours by it, and after
    # the fold it is the only title that still names a row's subject.
    return (
        decay.join(page_dim.select("page_id", "page_title"), "page_id")
        .select(
            "page_title", "hours_since_peak", "views", "peak_views", "pct_of_peak"
        )
        .orderBy("page_title", "hours_since_peak")
    )
