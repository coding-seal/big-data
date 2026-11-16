# Tasks

What's outstanding, carried between sessions. This file is the store of record — update it as work lands rather than leaving state in a session.

Finished work isn't logged here; the measurements and gotchas worth keeping go to `AGENTS.md` ("Known gotchas") or `docs/`, and the rest to git history.

Last updated: 2026-11-12.

## Next

1. **Decide what to do about `Main_Page` and automated traffic.** Two separate decisions, both now measured:
   - `Main_Page` is **not** dropped by the `with_page_id` fold (AGENTS.md was wrong about this and is corrected). It is the biggest article at 32.1M views, 6.4% of all article traffic, and front-page traffic has a different weekly rhythm than reading — which is what Q5 measures. Its 26 title variants do merge correctly; the question is only whether to count it as an article at all.
   - Automated traffic: `2014_European_Parliament_election` 482,797 views, `Neatsville,_Kentucky` 666,849 as the #2 article, and `Microdata_(HTML)` / `JSON-LD` / `RDFa` clustered at ~180K are crawler signatures — roughly 8–10 of the top 25 articles are implausible as readership. Filter or disclose; don't leave it implicit. (Fixing Q3's ranking, below, pushed most of this out of the *decay* chart, but it's still in Q4/Q5's totals.)

2. **Fix Q4's mixed denominators in code, not just caption.** `site.py`'s caption now *discloses* that `avg_views` is over all articles in the bucket while `avg_revisions` is only over the 3–43% `revision_dim` covers, and that the coverage gradient runs the same direction as the effect — but the tooltip itself still mixes them. Disclosure is a stopgap, not the fix.

3. **Verify the remaining unverified change.** `pipeline/silver.py::_row_count` (counts written Parquet rather than recomputing) still hasn't been exercised by a real `build-silver`. The other two items here are now proven: the `src/pipeline/` restructure ran a full enwiki `build-gold` end to end, and `sources/page_dump.py`'s explicit schemas are confirmed by all four enwiki row-count gates matching exactly.

   Run one Spark session at a time — the box is 15GB and an enwiki job holds ~6.8GB.

4. **Push `site/` and flip the GitHub Pages toggle.** The site generator, workflow, and content are built (see "Resolved" below) but `site/` hasn't been pushed yet, and the one-time repo Settings → Pages → Source: "GitHub Actions" switch hasn't been flipped. Until both happen there's a workflow file and a local directory, not a published site.

## Resolved (2026-11-12)

- **Q2's news test can now say "no".** `NEWS_MATCHED` requires a nearby ITN edit whose comment `[[links]]` the spiking article by title, not just a timestamp within ±24h. Verified on the enwiki week: all 20 top spikes now read `NEWS_ABSENT`, and independently re-checking every linked title across the *entire* downloaded ITN history (not just the ±24h window) confirms this isn't a matching bug — only one of the 20 (`78th_Primetime_Emmy_Awards`) was ever linked at all, 5 days after its spike.
- **Q3 now ranks by `peak / avg`, not absolute `peak_views`.** Verified on the enwiki week: `Main_Page` and the `2014_European_Parliament_election` scraper traffic are both gone from the top 10, replaced by `Indian_Institutes_of_Technology` and `Peccary` — exactly the pages the old ranking was excluding (see the now-removed item 2's measurement, still in git history).
- **`Living_people` excluded from Q1b.** `questions/categories.py::ADMINISTRATIVE_CATEGORIES` is a one-entry explicit list, anti-joined alongside `hidden_category_dim`. It's metadata (every biography of a living person carries it) rather than a subject, same editorial judgement the old name-regex made, now scoped to one named entry instead of 43 patterns. Verified on both profiles.
- **GitHub Pages site built.** `pipeline/site.py` (+ `build-site` subcommand, `just pipeline::site`) renders every `config*.yaml` profile's `data/gold/<wiki>/*.json` into a committed `site/` — one page, one section per wiki, vega-embed from CDN, each chart captioned with its window (read from the config, not hardcoded) and a denominator/caveat line. `.github/workflows/pages.yml` deploys `site/` on push to `master`. Not yet pushed — see item 4 above.

## Validated (enwiki, 2026-09-14..20, don't re-derive)

> **Stale as of the 2026-10-28 18:31 page-dims rebuild.** Everything below was measured against the *previous* `silver/en` dims. That rebuild (which added `hidden_category_dim`) re-wrote `category_dim` 183,795,286 → 209,368,878 and `redirect_dim` 11,466,832 → 12,038,184 from the same 2026-10-08 bronze with purely additive parser changes — i.e. the old numbers came from earlier bronze, not from a regression (see the skill's gate table). `page_dim` is unchanged at 7,249,474.
>
> Consequence: more redirects now resolve, so every view total below will come out **higher** than stated, and Q4/Q5's per-article figures shift with them. The *shapes* of the findings should hold; the digits need a re-run before publishing. Q1/Q1b are unaffected in method but their category totals change with the bigger `category_dim`.

A full `build-gold` ran clean on 2026-10-28 in **47 min** (not the ~85 previously estimated): q1 30 min, then q2 4, q3 7, q4 2, q5 4.

- Window is exactly right: 7 dates × 24 hours, 172,734,817 rows, and the windowed read equals the whole table — `silver/en` holds no stale partitions.
- All four silver row-count gates match the `.agents/skills/running-pipeline-jobs` table exactly.
- **Q4 and Q5 independently agree that total article traffic is 501,872,103 views.** Different aggregation paths, same total to the view — the strongest evidence the fold and the window are consistent.
- Q4 articles sum to 7,249,474 = `page_dim` exactly, so the left-join-from-`page_dim` zero fill is intact. Length effect **71.5x** (not the 80x from the contaminated run).
- Q5 weekend/weekday ratio is **0.794** (not 0.820), full 7×24 grid, Monday highest at 78.3M, Saturday lowest at 58.9M.
- Non-canonical titles carry 6.2% of enwiki article traffic (31,081,767 of 501,872,103).

## Decide

- **Materialize the `page_id` fold as a silver table.** Would remove the same join running 5× per `build-gold` (currently ~85 min for five questions). Real drawbacks: it fixes one granularity for every question, can't carry `page_title` so chart-labelling joins remain, makes the 7.2%-of-traffic redirect drop invisible to anything downstream, and adds a second derived table that goes stale from two upstreams. Needs a decision, not a drive-by optimization.

## Known gaps in the answers

- **Q1 has no topic taxonomy.** At enwiki scale **all 15** of the top categories by views are maintenance tags (`Articles_with_short_description`, 454M). `q1b_topic_time_filtered` now removes them by name convention (`questions/categories.py`), which is a floor, not a fix: what survives is still raw categories, far narrower than topics. The structural version is `page_props.sql.gz`'s `__HIDDENCAT__` flag; the real answer is still a Wikidata-backed `topic_dim`. Biggest remaining gap between claim and measurement, and it also affects the "by topic" half of Q5.

  **Resolved as of 2026-10-28:** `hidden_category_dim` (from `page_props.sql.gz`'s `__HIDDENCAT__`, 92,852 categories on enwiki) is now the filter, and the top 15 is finally free of tracking tags. It is also the fastest of the three versions — 17m27s vs 21m04s for the regex and 25m18s for unfiltered q1, since a `left_anti` on 92,852 names beats 43 regex alternatives over 209M rows.

  One thing remained after that, now also resolved (2026-11-12): **`Living_people` led at 82.3M views**, 9.6x the runner-up, because MediaWiki does not hide it: it is a real but *metadata* category carried by every biography of a living person (502,694 articles in the peak hour). It's now excluded via `ADMINISTRATIVE_CATEGORIES`, a one-entry explicit list — the same editorial judgement the old name-regex made, scoped to a single named category instead of 43 patterns. What's left is **American screen entertainment**, with `American_male_film_actors` / `..._television_actors` / `21st-century_...` / `20th-century_...` being the same people sliced four ways, so the `topic_dim` gap is untouched by any of this.

  The history, for anyone tempted to reach for a regex again. On q1b's first enwiki run the name filter dropped 84,845 of 2,053,869 distinct categories (4.1% of names, nearly all the volume), but **6 of the filtered top 15 were still tracking tags** — `Biography_with_signature` 12.98M, `Deprecated_archive_template_links` 11.72M, `Album_articles_lacking_alt_text_for_covers` 10.59M, `Commons_link_from_Wikidata` 7.51M, `Rotten_Tomatoes_ID_same_as_Wikidata` 6.07M, `Album_chart_called_without_album` 5.03M — because those anchor mid-string, not by prefix. Patterns for those six are in, and the re-run confirmed the loop rather than ending it: all six dropped, and **three new leaks surfaced from the next tier** — `Certification_Table_Entry_usages_for_United_Kingdom` 4.98M, `..._for_United_States` 4.66M, `IBDB_name_template_using_Wikidata` 4.53M (12 of 15 genuine, up from 8). These share no naming convention with the previous six, which is the whole point: hidden categories share a *flag*, not a name. **Stop adding patterns** — read `page_props.sql.gz`'s `__HIDDENCAT__` instead (new bronze file + silver table, one pass, ends it). Each regex round costs ~21 min to discover the next three leaks. The 8 genuine survivors in the first run were *all* American screen-actor categories sliced by medium/gender/century (`American_male_film_actors` 8.45M, `21st-century_American_male_actors` 7.27M, `American_film_actresses` 7.16M, ...): one population drawn as 7 overlapping series, which is the `topic_dim` gap in a single picture.
- **Q5 has n=1 per weekday.** One week cannot separate a real weekly pattern from a single viral page — and the 0.794 ratio is measured over a population that includes `Main_Page` (6.4% of traffic) and the crawler pages in task 3, whose weekly rhythm is not readership.
- **enwiki `revision_dim` is partial by construction.** Monthly history files only cover pages edited in the months downloaded, so enwiki's Q4 `avg_revisions` is not a whole-wiki average.

## Engineering

- **No tests.** Every bug found so far has been silent: plausible output, exit code 0. The parser and the silver-window logic are where this costs most.
- Cheaper wins not taken: `page_dump` parsing crosses the Python/JVM boundary once per row (183M times for `categorylinks`); a broadcast-threshold experiment for the title join (may OOM against the ~3.5GB execution budget).
- **Q1 is still the dominant cost** at ~25 min, even after the `(page_id, hour)` pre-aggregation landed (29m40s → 25m18s, ~15%; the 7x-fan-out arithmetic does not translate, see `questions/categories.py`). With `q1b` there are now two of them in a `build-gold`. Remaining levers, roughly by ratio:
  - `approx_count_distinct` for the `articles` column in q1/q1b/q5. It is the heaviest operator (~2M groups) and `articles` is a tooltip field, not a headline number — but it changes a reported figure by ~1–2%, so it needs a decision, not a drive-by.
  - `agg` in `views_by_category_hour` is consumed twice (top-N selection and the final join). Spark's `ReuseExchange` *may* collapse it since the subplans match, but nothing makes that explicit — check `explain()` before adding a `persist()`. `q3_decay` has the same shape with `ts`.
  - Share the `with_page_id` fold across questions: it now runs 8x per `build-gold`, not 5x. See the "Decide" item above — the middle path is persisting it for one run rather than materializing a silver table.
- Optionally reclaim ~36GB by deleting `data/bronze/enwiki/*.sql`; the `.gz` originals stay and builds just get slower.
