"""Gold charts -> a static GitHub Pages site.

No backend: this copies each already-built data/gold/<wiki>/<name>.json
into site/data/<wiki>/ (translating field titles and a few known data
values per site_content.yaml on the way) and renders site_template.html
(Jinja2) into site/index.html, embedding the charts client-side with
vega-embed (CDN, no bundler, see pipeline/charts.py). Doesn't touch Spark
-- run after `build-gold`, not instead of it.

Three layers, kept separate on purpose:
  - site_content.yaml (repo root): all page/chart text -- titles,
    summaries, caveats, wiki labels, field/value translations. Add a
    question's caption or fix a typo here; it names no Spark table, no
    silver/gold path, nothing about how the numbers were produced.
  - site_template.html: the HTML shell (Jinja2). Change layout/styling
    here without touching Python.
  - this module: orchestration only -- reads gold/*.json + site_content.yaml,
    applies the chart-legibility transforms below (faceting, translation),
    and renders the template. No chart prose or HTML lives in this file.

Color: a validated 8-hue categorical palette (ordered, never cycled --
see the project's dataviz conventions), kept here rather than in the
content config since it's a design-system/engineering parameter, not
page text. A color-encoded field with more than 8 distinct values can't
fit it without hues repeating indistinguishably, so `_facet_if_crowded`
turns those charts (Q1/Q1b's 15 categories, Q3's 10 pages) into small
multiples instead -- one subplot per value, no legend needed, and it
also fixes long category names getting clipped by a crowded legend.
"""
import json
import shutil
from pathlib import Path

import yaml
from jinja2 import Template

from config import Config, load_config

from .storage import REPO_ROOT

SITE_DIR = REPO_ROOT / "site"
GOLD_DIR = REPO_ROOT / "data" / "gold"
CONTENT_PATH = REPO_ROOT / "site_content.yaml"
TEMPLATE_PATH = Path(__file__).parent / "site_template.html"

VEGA_CDN = [
    "https://cdn.jsdelivr.net/npm/vega@5",
    "https://cdn.jsdelivr.net/npm/vega-lite@5",
    "https://cdn.jsdelivr.net/npm/vega-embed@6",
]

# 8 fixed hues, light/dark steps of the same palette -- order is the
# colorblind-safety mechanism, never cycle past it (see dataviz skill's
# palette.md). Charts with more distinct categories than this are faceted
# instead, below.
PALETTE_LIGHT = [
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100",
    "#e87ba4", "#008300", "#4a3aa7", "#e34948",
]
PALETTE_DARK = [
    "#3987e5", "#d95926", "#199e70", "#c98500",
    "#d55181", "#008300", "#9085e9", "#e66767",
]
MAX_CATEGORICAL = len(PALETTE_LIGHT)
# Vega-Lite renders a fixed pixel size, not a responsive one -- sized so
# FACET_COLUMNS * (FACET_WIDTH + axis/title chrome) stays under the
# .chart-card's ~820px inner width even for q1's wide "20,000,000"-style
# y-axis labels. Measured overflow at columns=4/width=150 (~1150px
# rendered): shrink the knobs, don't just rely on the CSS overflow fallback.
FACET_COLUMNS = 3
FACET_WIDTH = 130
FACET_HEIGHT = 100

VEGA_CONFIG_LIGHT = {
    "background": "#fcfcfb",
    "mark": {"color": PALETTE_LIGHT[0]},
    "range": {"category": PALETTE_LIGHT},
    "axis": {
        "labelColor": "#52514e", "titleColor": "#0b0b0b",
        "domainColor": "#c3c2b7", "tickColor": "#c3c2b7", "gridColor": "#e1e0d9",
        "labelLimit": 500,
    },
    "legend": {"labelColor": "#52514e", "titleColor": "#0b0b0b", "labelLimit": 500},
    "header": {"labelColor": "#0b0b0b", "titleColor": "#0b0b0b", "labelLimit": 500},
    "title": {"color": "#0b0b0b"},
    "view": {"stroke": "transparent"},
}
VEGA_CONFIG_DARK = {
    "background": "#1a1a19",
    "mark": {"color": PALETTE_DARK[0]},
    "range": {"category": PALETTE_DARK},
    "axis": {
        "labelColor": "#c3c2b7", "titleColor": "#ffffff",
        "domainColor": "#383835", "tickColor": "#383835", "gridColor": "#2c2c2a",
        "labelLimit": 500,
    },
    "legend": {"labelColor": "#c3c2b7", "titleColor": "#ffffff", "labelLimit": 500},
    "header": {"labelColor": "#ffffff", "titleColor": "#ffffff", "labelLimit": 500},
    "title": {"color": "#ffffff"},
    "view": {"stroke": "transparent"},
}


def _profiles() -> dict[str, Config]:
    """Every config*.yaml at the repo root, keyed by the wiki it names --
    gives each gold/<wiki> directory a window to caption itself with
    without hardcoding wiki/date facts here."""
    return {
        config.wiki: config
        for config in (load_config(path) for path in sorted(REPO_ROOT.glob("config*.yaml")))
    }


def _translate_spec(spec: dict, field_titles: dict, value_translations: dict) -> dict:
    """English field names/titles and a few English data values ->
    whatever site_content.yaml says, for display only."""
    spec = dict(spec)
    spec.pop("title", None)  # the HTML card's own <h3> carries the title

    encoding = dict(spec.get("encoding", {}))
    for name, channel in encoding.items():
        if name == "tooltip":
            continue
        channel = dict(channel)
        field = channel.get("field")
        if field in field_titles:
            channel["title"] = field_titles[field]
        if field in value_translations and "sort" in channel:
            channel["sort"] = [value_translations[field].get(v, v) for v in channel["sort"]]
        encoding[name] = channel
    if "tooltip" in encoding:
        encoding["tooltip"] = [
            {**entry, "title": field_titles[entry["field"]]}
            if entry.get("field") in field_titles else entry
            for entry in encoding["tooltip"]
        ]
    spec["encoding"] = encoding

    values = []
    for row in spec["data"]["values"]:
        row = dict(row)
        for field, mapping in value_translations.items():
            if field in row:
                row[field] = mapping.get(row[field], row[field])
        if "is_weekend" in row:
            row["is_weekend"] = "Да" if row["is_weekend"] else "Нет"
        values.append(row)
    spec["data"] = {"values": values}

    return spec


def _facet_if_crowded(spec: dict) -> dict:
    """A color-encoded field with more than MAX_CATEGORICAL distinct values
    can't fit the fixed 8-hue palette without colors repeating
    indistinguishably (Q1/Q1b's ~15 categories, Q3's 10 pages) -- fold it
    into small multiples instead: one subplot per value, single series each
    so no legend is needed, and no long category name fighting a legend
    for space either."""
    color = spec.get("encoding", {}).get("color")
    if not color or "field" not in color:
        return spec
    field = color["field"]
    distinct = sorted({row[field] for row in spec["data"]["values"] if field in row})
    if len(distinct) <= MAX_CATEGORICAL:
        return spec

    inner_encoding = {k: v for k, v in spec["encoding"].items() if k != "color"}
    faceted = {
        "$schema": spec["$schema"],
        "data": spec["data"],
        "facet": {"field": field, "type": "nominal", "sort": distinct},
        "columns": FACET_COLUMNS,
        "resolve": {"scale": {"y": "independent"}},
        "spec": {
            "width": FACET_WIDTH, "height": FACET_HEIGHT,
            "mark": spec["mark"], "encoding": inner_encoding,
        },
    }
    if color.get("title"):
        faceted["facet"]["title"] = color["title"]
    return faceted


def build() -> None:
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    (SITE_DIR / "data").mkdir(parents=True)

    if not GOLD_DIR.exists():
        raise FileNotFoundError(f"no gold output under {GOLD_DIR} -- run build-gold first")
    wikis = sorted(p.name for p in GOLD_DIR.iterdir() if p.is_dir())
    if not wikis:
        raise FileNotFoundError(f"no gold output under {GOLD_DIR} -- run build-gold first")

    content = yaml.safe_load(CONTENT_PATH.read_text())
    field_titles = content.get("field_titles", {})
    value_translations = content.get("value_translations", {})
    wiki_labels = content.get("wiki_labels", {})
    questions = content.get("questions", {})

    profiles = _profiles()
    sections = []
    specs: list[dict[str, str]] = []
    for wiki in wikis:
        wiki_dir = SITE_DIR / "data" / wiki
        wiki_dir.mkdir(parents=True)
        config = profiles.get(wiki)
        window = f"{config.date_range.start} .. {config.date_range.end}" if config else "unknown"

        charts = []
        for src in sorted((GOLD_DIR / wiki).glob("*.json")):
            spec = json.loads(src.read_text())
            spec = _facet_if_crowded(_translate_spec(spec, field_titles, value_translations))
            (wiki_dir / src.name).write_text(json.dumps(spec))

            name = src.stem
            text = questions.get(name, {})
            chart_id = f"chart-{wiki}-{name}"
            charts.append({
                "id": chart_id,
                "title": text.get("title", name),
                "summary": text.get("summary", ""),
                "caveat": text.get("caveat", ""),
            })
            specs.append({"id": chart_id, "url": f"data/{wiki}/{src.name}"})

        sections.append({
            "id": wiki, "label": wiki_labels.get(wiki, wiki), "window": window, "charts": charts,
        })

    html = Template(TEMPLATE_PATH.read_text()).render(
        page=content["page"],
        sections=sections,
        vega_cdn=VEGA_CDN,
        specs_json=json.dumps(specs),
        light_config_json=json.dumps(VEGA_CONFIG_LIGHT),
        dark_config_json=json.dumps(VEGA_CONFIG_DARK),
    )
    (SITE_DIR / "index.html").write_text(html)
