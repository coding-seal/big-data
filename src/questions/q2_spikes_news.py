"""
Q2: traffic spikes vs. news -- detect spikes in pageviews_fact, then flag
which ones fall near an edit to the wiki's "In the news" style page
(itn_edit_fact, from sources/mediawiki_history.py).

Spike detection needs no new source; only the "is this near real news"
half does. Matching is a heuristic (nearest ITN edit within a time
window), not a derivable fact -- see docs/sources/wikinews-itn.md.

**"Not near news" and "no news data" are different answers** and this
module keeps them apart, because two separate things routinely leave
itn_edit_fact without an edit to match against:

  - `mediawiki_history.itn_page_title` isn't set (true for the default
    `simple` config -- simplewiki has no actively-maintained ITN page, see
    docs/sources/wikinews-itn.md), so the table is empty outright;
  - the dumps lag. mediawiki_history publishes monthly snapshots a few
    weeks behind: the 2026-09 snapshot was generated 2026-10-05 and its
    2026-10 file is a 1.4MB stub covering only the first days of October,
    while the pageviews files are current to yesterday. A window the
    history doesn't reach has no ITN edits *available*, which is not the
    same as a window with no news in it.

Collapsing either case into has_nearby_news=false would manufacture a
finding ("most spikes aren't news-driven!") out of missing data, so spikes
outside the span itn_edit_fact actually covers are labelled explicitly.

**A match requires the ITN edit to name the article, not just be nearby in
time.** The ITN page is edited ~6.6x/day, so on a one-week window every
single hour sits within +-24h of *some* edit -- a timestamp-only match is
NEWS_MATCHED for 168/168 hours by arithmetic, which carries no information
(measured on the enwiki week, see AGENTS.md). mediawiki_history doesn't give
the full wikitext, but it does give the edit comment, and ITN edit comments
conventionally name the articles being added/removed as `[[Title]]`
wikilinks ("RD: + *[[Albert Alós]]", "+2026 Kazakh legislative election").
So NEWS_MATCHED now requires a nearby edit whose comment links the spiking
article by title -- a real, falsifiable test instead of a proximity count
that can only ever say yes.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from questions.pages import ARTICLE_HOUR_INPUTS, article_hours

# A hour counts as a "spike" for a page if its views are at least this many
# times that page's own average views over the window.
SPIKE_RATIO = 3.0
# How close (in hours) a linking ITN edit must be to a spike to count as
# "nearby".
NEWS_WINDOW_HOURS = 24
# Same reasoning as q1/q3's top-N caps.
TOP_N_SPIKES = 20
USES_ARTICLE_HOURS = True
# Captures "Title" out of "[[Title]]" or "[[Title|display text]]".
# shortcut: exact-case match only (no first-letter case-folding), so a link
# typed with a different first-letter case than page_dim's title silently
# misses; widen with F.initcap on both sides if false negatives show up.
ITN_LINK_PATTERN = r"\[\[([^\]|]+)"

# The three answers this question can give about one spike.
NEWS_MATCHED = "near an ITN edit"
NEWS_ABSENT = "no ITN edit nearby"
NEWS_UNKNOWN = "no ITN data for this hour"


def required_tables() -> list[str]:
    return [*ARTICLE_HOUR_INPUTS, "itn_edit_fact"]


def chart_encoding() -> dict:
    return {
        "x": {"field": "ts", "type": "temporal", "title": "Spike hour"},
        "y": {"field": "views", "type": "quantitative"},
        "color": {
            "field": "news_match", "type": "nominal",
            "sort": [NEWS_MATCHED, NEWS_ABSENT, NEWS_UNKNOWN],
        },
        "tooltip": [
            {"field": "page_title"}, {"field": "ts"}, {"field": "views"},
            {"field": "avg_views"}, {"field": "hours_from_nearest_news"},
        ],
    }


def default_mark() -> str:
    return "point"


def run(tables: dict[str, DataFrame]) -> DataFrame:
    itn = tables["itn_edit_fact"]
    page_dim = tables["page_dim"]

    # A window computes the baseline over the observed article-hours
    # without joining the full hourly series back to itself.
    spikes = (
        article_hours(tables)
        .withColumn("avg_views", F.avg("views").over(Window.partitionBy("page_id")))
        .filter(F.col("avg_views") > 0)
        .filter(F.col("views") >= SPIKE_RATIO * F.col("avg_views"))
        .join(page_dim.select("page_id", "page_title"), "page_id")
        .select("page_title", "ts", "views", "avg_views")
        .orderBy(F.desc("views"), "page_title", "ts")
        .limit(TOP_N_SPIKES)
    )

    # mediawiki_history stores the raw event_timestamp string (e.g.
    # "2026-09-03 12:34:56.0"); truncate to whole seconds before parsing
    # since the fractional-second digit count varies row to row. The
    # linked_titles array turns "[[Title]]"/"[[Title|display]]" wikilinks in
    # the edit comment into underscore-joined titles matching page_dim's
    # format (see module docstring).
    itn_links = itn.select(
        F.to_timestamp(F.substring("event_timestamp", 1, 19), "yyyy-MM-dd HH:mm:ss").alias(
            "news_ts"
        ),
        F.transform(
            F.regexp_extract_all("event_comment", F.lit(ITN_LINK_PATTERN), 1),
            lambda link: F.regexp_replace(F.trim(link), " ", "_"),
        ).alias("linked_titles"),
    )

    # News status does not affect rank. Match only the selected spikes and
    # retain unmatched rows in the same join.
    linked_edits = itn_links.select(
        "news_ts", F.explode("linked_titles").alias("page_title")
    ).alias("edit")
    hours_from_news = (
        F.abs(F.col("spike.ts").cast("long") - F.col("edit.news_ts").cast("long"))
        / 3600
    )
    nearest_news = (
        spikes.alias("spike").join(
            linked_edits,
            (F.col("spike.page_title") == F.col("edit.page_title"))
            & (hours_from_news <= NEWS_WINDOW_HOURS),
            "left",
        )
        .select("spike.*", hours_from_news.alias("hours_from_news"))
        .groupBy("page_title", "ts", "views", "avg_views")
        .agg(F.min("hours_from_news").alias("hours_from_nearest_news"))
    )

    # The span itn_edit_fact can speak to at all: outside it, "no linking
    # edit nearby" says something about the dump's coverage, not about the
    # news. Both bounds are null when itn is empty, so every comparison below
    # is null and every spike falls through to UNKNOWN -- which is the
    # honest answer when no ITN page is configured.
    bounds = itn_links.agg(
        F.min("news_ts").alias("news_from"), F.max("news_ts").alias("news_to")
    )
    window = F.expr(f"interval {NEWS_WINDOW_HOURS} hours")

    return (
        nearest_news.crossJoin(bounds)
        .withColumn(
            "news_match",
            F.when(F.col("hours_from_nearest_news").isNotNull(), F.lit(NEWS_MATCHED))
            .when(
                F.col("ts").between(
                    F.col("news_from") - window, F.col("news_to") + window
                ),
                F.lit(NEWS_ABSENT),
            )
            .otherwise(F.lit(NEWS_UNKNOWN)),
        )
        .drop("news_from", "news_to")
        .orderBy(F.desc("views"), "page_title", "ts")
    )
