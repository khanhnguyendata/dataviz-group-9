"""Explanatory Marimo notebook: planned vs visited fishing/tourism places.

Export to PDF (A4 landscape, half-page friendly):
1. Run this notebook (`marimo edit khanh_marimo_explainer.py`).
2. Right-click the rendered iframe and choose "Open Frame" (or just print the page).
3. File -> Print -> Save as PDF -> Paper: A4, Orientation: Landscape, Margins: None.
The figure is sized to A4-landscape proportions, so the result drops into a
report at A5-landscape (half of an A4 portrait page) without distortion.
"""

import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    import html as _html
    from collections import defaultdict

    import pandas as pd

    JD = "data/Collected_by_the_Journalist/"

    # ------------------------------------------------------------------ data
    def read_csv(path: str) -> pd.DataFrame:
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    people_df = read_csv(JD + "people.csv")
    plans_df = read_csv(JD + "plans.csv")
    places_df = read_csv(JD + "places.csv")
    ppp_df = read_csv(JD + "plan_people_participations.csv")
    travel_links_df = read_csv(JD + "travel_links.csv")
    trip_people_df = read_csv(JD + "trip_people.csv")
    trip_places_df = read_csv(JD + "trip_places.csv")

    # ------------------------------------------------------------ design tokens
    # Blue = industrial / fishing.  Orange = tourism.
    INDUSTRIAL = "#2563eb"           # blue-600
    INDUSTRIAL_LIGHT = "#dbeafe"     # blue-100
    INDUSTRIAL_MID = "#bfdbfe"       # blue-200
    INDUSTRIAL_INK = "#1e3a8a"       # blue-900
    INDUSTRIAL_BAR_FILL = "#93c5fd"  # blue-300
    TOURISM = "#ea580c"              # orange-600
    TOURISM_LIGHT = "#ffedd5"        # orange-100
    TOURISM_MID = "#fed7aa"          # orange-200
    TOURISM_INK = "#7c2d12"          # orange-900
    TOURISM_BAR_FILL = "#fdba74"     # orange-300

    NEUTRAL_DARK = "#94a3b8"   # slate-400 (gray line for planned-not-visited)
    NEUTRAL_LIGHT = "#e2e8f0"  # slate-200 (zero reference line, unvisited)
    TICK_COLOR = "#1e293b"
    INK = "#0f172a"
    MUTED = "#64748b"

    SCALE_MAX = 6  # full board

    # --------------------------------------------------------- restrict scope
    KEPT_ZONES = {"industrial", "tourism"}
    zone_places_df = places_df[places_df["zone"].isin(KEPT_ZONES)].copy()
    named_zone = zone_places_df[zone_places_df["name"] != ""].copy()

    named_place_ids = set(named_zone["place_id"])
    all_zone_place_ids = set(zone_places_df["place_id"])

    travel_plans = plans_df[plans_df["plan_type"].str.title() == "Travel"].copy()
    travel_plan_ids = set(travel_plans["plan_id"])

    kept_tl = travel_links_df[
        travel_links_df["plan_id"].isin(travel_plan_ids)
        & travel_links_df["place_id"].isin(named_place_ids)
    ].copy()
    kept_plan_ids = set(kept_tl["plan_id"])

    plan_people: dict[str, set[str]] = defaultdict(set)
    for _, r in ppp_df.iterrows():
        plan_id = str(r["plan_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if plan_id in kept_plan_ids and person_id:
            plan_people[plan_id].add(person_id)

    plan_to_place = {
        str(r["plan_id"]).strip(): str(r["place_id"]).strip()
        for _, r in kept_tl.iterrows()
    }

    planned_per_place: dict[str, set[str]] = defaultdict(set)
    for plan_id, place_id in plan_to_place.items():
        planned_per_place[place_id] |= plan_people.get(plan_id, set())

    visited_per_place: dict[str, set[str]] = defaultdict(set)
    trip_merged = trip_people_df.merge(trip_places_df, on="trip_id", how="inner")
    for _, r in trip_merged.iterrows():
        place_id = str(r["place_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if place_id in all_zone_place_ids and person_id:
            visited_per_place[place_id].add(person_id)

    # ----------------------------------------------------- people entries
    people_entries = []
    for _, r in people_df.iterrows():
        pid = str(r["people_id"]).strip()
        name = str(r["name"]).strip()
        people_entries.append({"id": pid, "label": name})
    _PEOPLE_FIRST = ["Tante Titan", "Seal", "Carol Limpet"]
    people_entries.sort(
        key=lambda e: (
            _PEOPLE_FIRST.index(e["label"]) if e["label"] in _PEOPLE_FIRST else len(_PEOPLE_FIRST),
            e["label"].lower(),
        )
    )

    # ----------------------------------------------------- place entries (named only)
    def make_named_place_entry(row):
        place_id = str(row["place_id"]).strip()
        zone = str(row["zone"]).strip()
        planners = planned_per_place.get(place_id, set())
        visitors = visited_per_place.get(place_id, set())
        both = planners & visitors
        unplanned = visitors - planners
        return {
            "id": place_id,
            "label": str(row["name"]).strip(),
            "group": zone,
            "zone": zone,
            "is_anchor": False,
            "is_aggregate": False,
            "planned": len(planners),
            "visited": len(visitors),
            "both": len(both),
            "unplanned_visited": len(unplanned),
            "planners": planners,
            "visitors": visitors,
            "row_class": (
                "row-industrial" if zone == "industrial" else "row-tourism"
            ),
        }

    place_entries: list[dict] = []
    for _, row in named_zone.iterrows():
        place_entries.append(make_named_place_entry(row))

    ZONE_ORDER = {"industrial": 0, "tourism": 1}
    _PLACE_PRIORITY = {
        "Sailor's Perch Light": 0,
        "Harbor's Edge Grill":  1,
        "Captain's Market":     2,
    }
    place_entries.sort(
        key=lambda e: (
            ZONE_ORDER.get(e["zone"], 9),
            -e["visited"],
            -e["planned"],
            _PLACE_PRIORITY.get(e["label"], 999),
            e["label"].lower(),
        )
    )

    unnamed_to_agg: dict[str, str] = {}

    # ----------------------------------------------------- edges
    place_index = {e["id"]: e for e in place_entries}
    valid_people = {e["id"] for e in people_entries}

    person_place_state: dict[tuple[str, str], dict[str, bool]] = defaultdict(
        lambda: {"planned": False, "visited": False}
    )

    for place_id, planners in planned_per_place.items():
        target = place_id if place_id in named_place_ids else unnamed_to_agg.get(place_id)
        if not target or target not in place_index:
            continue
        for person_id in planners:
            if person_id in valid_people:
                person_place_state[(person_id, target)]["planned"] = True

    for place_id, visitors in visited_per_place.items():
        target = place_id if place_id in named_place_ids else unnamed_to_agg.get(place_id)
        if not target or target not in place_index:
            continue
        for person_id in visitors:
            if person_id in valid_people:
                person_place_state[(person_id, target)]["visited"] = True

    edges: list[dict] = []
    for (person_id, target), st in person_place_state.items():
        if not st["planned"] and not st["visited"]:
            continue
        if st["planned"] and st["visited"]:
            style = "e-both"
        elif st["planned"]:
            style = "e-planned-only"
        else:
            style = "e-unplanned"
        edges.append(
            {
                "a_id": person_id,
                "b_id": target,
                "style": style,
                "zone": place_index[target]["zone"],
            }
        )

    import json as _json
    payload = _json.dumps({"edges": edges}, ensure_ascii=False)

    # ----------------------------------------------------- bullet SVG helper
    def bullet_svg(entry: dict, annotate: bool = False, annotate_tour: bool = False) -> str:
        """Bullet bar with per-element count labels and a zero reference line.

        annotate=True (first industrial row) uses verbose labels:
          "N planned" instead of "N", "0 visited" instead of "0".
        annotate_tour=True (first tourism row) shows:
          "N planned" right of tick, "N visited" above bar end, and
          "(including N unplanned)" below bar centered on unplanned segment.
        """
        both_count = entry["both"]
        unplanned = entry["unplanned_visited"]
        planned_total = entry["planned"]
        visited_total = both_count + unplanned
        zone = entry["zone"]

        width = 410
        height = 34
        pad_left = 10
        unit = 50
        bar_h = 14
        bar_y = (height - bar_h) // 2   # 10
        tick_extra = 0   # ticks and bar share the same vertical extent

        fill = INDUSTRIAL_BAR_FILL if zone == "industrial" else TOURISM_BAR_FILL
        stroke = INDUSTRIAL if zone == "industrial" else TOURISM

        x_solid = pad_left
        w_solid = unit * both_count
        x_dotted = pad_left + w_solid
        w_dotted = unit * unplanned
        tick_x = pad_left + unit * planned_total
        label_y = bar_y + bar_h / 2 + 3.5

        parts = [
            f'<svg class="bullet" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" preserveAspectRatio="xMinYMid meet">'
        ]

        # Zero reference line: accent color when visited=0, faint otherwise.
        zero_color = stroke if visited_total == 0 else NEUTRAL_LIGHT
        parts.append(
            f'<line x1="{pad_left}" x2="{pad_left}" '
            f'y1="{bar_y - tick_extra}" y2="{bar_y + bar_h + tick_extra}" '
            f'stroke="{zero_color}" stroke-width="1.5" stroke-linecap="round" />'
        )

        # Solid bar (planned and visited) — darker accent fill, no outline.
        if w_solid > 0:
            parts.append(
                f'<rect x="{x_solid}" y="{bar_y}" width="{w_solid}" height="{bar_h}" '
                f'fill="{stroke}" stroke="none" rx="1.5" />'
            )
        # Unplanned-visit bar — lighter fill, no outline.
        if w_dotted > 0:
            parts.append(
                f'<rect x="{x_dotted}" y="{bar_y}" width="{w_dotted}" height="{bar_h}" '
                f'fill="{fill}" stroke="none" rx="1.5" />'
            )

        # Plan tick.
        parts.append(
            f'<line x1="{tick_x}" x2="{tick_x}" '
            f'y1="{bar_y - tick_extra}" y2="{bar_y + bar_h + tick_extra}" '
            f'stroke="{TICK_COLOR}" stroke-width="2" stroke-linecap="round" />'
        )

        # Planned label — right of tick, vertically centred on bar.
        planned_text = f"{planned_total} planned" if (annotate or annotate_tour) else str(planned_total)
        parts.append(
            f'<text x="{tick_x + 4}" y="{label_y}" '
            f'font-size="11" text-anchor="start" '
            f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif" '
            f'fill="{TICK_COLOR}" font-weight="600">{planned_text}</text>'
        )

        # Zero-visit label — right of zero reference line (only when visited=0).
        if visited_total == 0:
            zero_text = "0 visited" if annotate else "0"
            parts.append(
                f'<text x="{pad_left + 4}" y="{label_y}" '
                f'font-size="11" text-anchor="start" '
                f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif" '
                f'fill="{stroke}" font-weight="600">{zero_text}</text>'
            )

        # Visited label — right of bar end (only when visited > 0).
        if visited_total > 0 and not annotate_tour:
            visited_label_x = pad_left + unit * visited_total + 6
            parts.append(
                f'<text x="{visited_label_x}" y="{label_y}" '
                f'font-size="11" text-anchor="start" '
                f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif" '
                f'fill="{stroke}" font-weight="600">{visited_total}</text>'
            )

        # Annotated tourism labels — two lines stacked right of bar end.
        if annotate_tour and visited_total > 0:
            bar_end_x = pad_left + unit * visited_total
            font_family = "-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif"
            # Line 1: "N visited" — vertically centred on bar.
            parts.append(
                f'<text x="{bar_end_x + 6}" y="{label_y}" '
                f'font-size="11" text-anchor="start" '
                f'font-family="{font_family}" '
                f'fill="{stroke}" font-weight="600">{visited_total} visited</text>'
            )
            # Line 2: "(N unplanned)" — one line below.
            parts.append(
                f'<text x="{bar_end_x + 6}" y="{label_y + 13}" '
                f'font-size="11" text-anchor="start" '
                f'font-family="{font_family}" '
                f'fill="{fill}" font-weight="600">({unplanned} not planned)</text>'
            )

        parts.append("</svg>")
        return "\n".join(parts)

    _first_tour = next(
        (i for i, e in enumerate(place_entries) if e["zone"] == "tourism"), None
    )
    place_bullet_html = {
        e["id"]: bullet_svg(
            e,
            annotate=(i == 0),
            annotate_tour=(i == _first_tour),
        )
        for i, e in enumerate(place_entries)
    }

    # ----------------------------------------------------- column rendering
    def grouped_column_html(entries: list[dict], render_row, group_label_of) -> str:
        out: list[str] = ['<div class="list">']
        current_group = None
        for e in entries:
            if e["group"] != current_group:
                if current_group is not None:
                    out.append("</div>")
                current_group = e["group"]
                out.append('<div class="group">')
                out.append(
                    f'<div class="group-header" data-zone="{e.get("zone", "")}">'
                    f'{group_label_of(e)}</div>'
                )
            out.append(render_row(e))
        if current_group is not None:
            out.append("</div>")
        out.append("</div>")
        return "\n".join(out)

    def flat_people_html(entries: list[dict]) -> str:
        parts = ['<div class="list flat-list">']
        for e in entries:
            parts.append(
                f'<div class="row row-neutral" data-table="people" '
                f'data-id="{_html.escape(e["id"])}">{_html.escape(e["label"])}</div>'
            )
        parts.append("</div>")
        return "\n".join(parts)

    def place_row_html(e: dict) -> str:
        return (
            f'<div class="row {e["row_class"]}" data-zone="{e["zone"]}" '
            f'data-table="places" data-id="{_html.escape(e["id"])}">'
            f'{_html.escape(e["label"])}'
            "</div>"
        )

    def bullet_row_html(e: dict) -> str:
        return (
            f'<div class="row bullet-row" data-zone="{e["zone"]}" '
            f'data-table="bullet" data-id="{_html.escape(e["id"])}">'
            f'{place_bullet_html[e["id"]]}'
            "</div>"
        )

    def zone_group_label(e: dict) -> str:
        if e["zone"] == "industrial":
            return (
                'Industrial sites'
                '<br><span class="group-subhead">(assumed fishing-related)</span>'
            )
        return "Tourism sites"

    people_html = flat_people_html(people_entries)
    places_html = grouped_column_html(place_entries, place_row_html, zone_group_label)
    bullet_html = grouped_column_html(place_entries, bullet_row_html, zone_group_label)

    # ----------------------------------------------------- legend
    def lg_swatch_solid_colored() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<rect x="1" y="2.5" width="11" height="5" fill="{INDUSTRIAL}" rx="2.5"/>'
            f'<rect x="16" y="2.5" width="11" height="5" fill="{TOURISM}" rx="2.5"/>'
            "</svg>"
        )

    def lg_swatch_light_solid() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<rect x="1" y="2.5" width="11" height="5" fill="{INDUSTRIAL_BAR_FILL}" rx="2.5"/>'
            f'<rect x="16" y="2.5" width="11" height="5" fill="{TOURISM_BAR_FILL}" rx="2.5"/>'
            "</svg>"
        )

    def lg_swatch_gray() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<line x1="0" x2="28" y1="5" y2="5" stroke="{NEUTRAL_DARK}" stroke-width="3" stroke-linecap="round"/>'
            "</svg>"
        )

    legend_html = (
        '<span class="lg-item">' + lg_swatch_solid_colored()  + '<span class="lg-label">Planned and visited</span></span>'
        '<span class="lg-item">' + lg_swatch_light_solid()    + '<span class="lg-label">Not planned but visited</span></span>'
        '<span class="lg-item">' + lg_swatch_gray()           + '<span class="lg-label">Planned but not visited</span></span>'
    )

    title_html = (
        'Clear bias in visits: all <span class="t-ind">fishing sites</span> are planned but unvisited '
        'by board members,<br>'
        'while some <span class="t-tour">tourism sites</span> are regularly visited outside of plans.'
    )
    footnote_html = (
        "Industrial &amp; Tourism sites only \u00b7 Unnamed places removed \u00b7 Source: journalist data"
    )

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  :root {
    --ind:       __IND__;
    --ind-light: __IND_LIGHT__;
    --ind-ink:   __IND_INK__;
    --tour:       __TOUR__;
    --tour-light: __TOUR_LIGHT__;
    --tour-ink:   __TOUR_INK__;
    --neutral-dark:  __NEU_DARK__;
    --neutral-light: __NEU_LIGHT__;
    --ind-bar:  __IND_BAR__;
    --tour-bar: __TOUR_BAR__;
    --ind-mid:  __IND_MID__;
    --tour-mid: __TOUR_MID__;
    --ink:   #0f172a;
    --slate: #475569;
    --muted: #64748b;
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0;
    background: #f1f5f9;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    color: var(--ink);
  }
  .page {
    /* A4 landscape at 96 dpi: 1122 x 794 CSS px (aspect 1.414:1). */
    width: 1122px;
    height: 794px;
    margin: 0 auto;
    padding: 18px 22px 14px 22px;
    background: #ffffff;
    box-shadow: 0 4px 18px rgba(15, 23, 42, 0.10);
    display: grid;
    grid-template-rows: auto auto 1fr auto;
    grid-template-columns: 1fr;
    row-gap: 8px;
    overflow: hidden;
  }
  .title {
    font-size: 19px;
    line-height: 1.35;
    font-weight: 700;
    letter-spacing: -0.005em;
    color: var(--ink);
    margin: 0;
  }
  .title .t-ind  { color: var(--ind); }
  .title .t-tour { color: var(--tour); }
  .legend-row {
    display: flex; align-items: center; flex-wrap: wrap;
    font-size: 11px; color: var(--slate);
    gap: 18px;
    margin-top: 2px;
  }
  .legend-row .lg-item {
    display: inline-flex; align-items: center; gap: 6px;
  }
  .legend-row .lg-svg { vertical-align: middle; display: inline-block; }
  .legend-row .lg-label { color: var(--slate); }
  .columns-wrap {
    position: relative;
    display: grid;
    grid-template-columns: 140px 1fr 180px 420px;
    gap: 32px;
    align-items: stretch;
    min-height: 0;
    padding-top: 4px;
  }
  .col {
    display: flex; flex-direction: column;
    justify-content: center;
    min-height: 0;
    min-width: 0;
  }
  .col[data-table="people"] { grid-column: 1; }
  .col[data-table="places"] { grid-column: 3; }
  .col[data-table="bullet"] { grid-column: 4; }
  .col-inner {
    display: flex; flex-direction: column;
  }
  .col-head {
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--muted);
    padding-left: 4px;
    margin-bottom: 6px;
  }
  .list {
    display: flex; flex-direction: column;
    gap: 8px;
    min-height: 0;
  }
  .list.flat-list { gap: 0; }
  .group {
    display: flex; flex-direction: column;
  }
  .group-header {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: var(--muted);
    padding: 1px 4px 2px 4px;
    margin-bottom: 4px;
  }
  .group-header[data-zone="industrial"] { color: var(--ind); }
  .group-header[data-zone="tourism"]    { color: var(--tour); }
  .group-subhead {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: var(--ind);
    display: block;
    margin-top: 1px;
  }
  .row {
    font-size: 12px;
    line-height: 16px;
    height: 34px;
    padding: 4px 8px;
    margin-bottom: 1px;
    border-radius: 4px;
    color: var(--ink);
    background: transparent;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    align-items: center;
  }
  .row.row-industrial {
    background: var(--ind-mid);
    color: var(--ind);
  }
  .row.row-tourism {
    background: var(--tour-mid);
    color: var(--tour);
  }
  .row.row-neutral {
    background: #f8fafc;
    color: var(--ink);
  }
  .col[data-table="bullet"] .row {
    background: transparent;
    padding: 0;
    overflow: visible;
  }
  .col[data-table="bullet"] .group-header {
    visibility: hidden;
  }
  .bullet { display: block; }
  /* Edge lines on the overlay SVG. */
  .overlay {
    position: absolute; top: 0; left: 0;
    width: 100%; height: 100%;
    pointer-events: none;
  }
  .overlay path { fill: none; stroke-linecap: round; }
  .overlay path.e-planned-only {
    stroke: var(--neutral-dark);
    stroke-opacity: 0.6;
    stroke-width: 1.3;
  }
  .overlay path.e-both {
    stroke-opacity: 0.9;
    stroke-width: 2;
  }
  .overlay path.e-both.zone-industrial { stroke: var(--ind); }
  .overlay path.e-both.zone-tourism    { stroke: var(--tour); }
  .overlay path.e-unplanned {
    stroke-opacity: 1;
    stroke-width: 2;
  }
  .overlay path.e-unplanned.zone-industrial { stroke: var(--ind-bar); }
  .overlay path.e-unplanned.zone-tourism    { stroke: var(--tour-bar); }
  .footnote {
    font-size: 10px;
    color: var(--muted);
    text-align: left;
    margin-top: 2px;
    padding-top: 4px;
    border-top: 1px solid #e2e8f0;
  }

  @page { size: A4 landscape; margin: 0; }
  @media print {
    html, body { background: #ffffff; }
    .page { box-shadow: none; }
  }
</style>
</head>
<body>
<div class="page" id="page">
  <h1 class="title">__TITLE__</h1>
  <div class="legend-row">__LEGEND__</div>
  <div class="columns-wrap" id="columns">
    <div class="col" data-table="people">
      <div class="col-inner">
        <div class="col-head">Board members</div>
        __PEOPLE__
      </div>
    </div>
    <div class="col" data-table="places">
      <div class="col-inner">
        <div class="col-head">Places</div>
        __PLACES__
      </div>
    </div>
    <div class="col" data-table="bullet">
      <div class="col-inner">
        <div class="col-head">Members planned vs visited each place</div>
        __BULLET__
      </div>
    </div>
    <svg class="overlay" id="overlay" aria-hidden="true"></svg>
  </div>
  <div class="footnote">__FOOTNOTE__</div>
</div>
<script>
(() => {
  const DATA = __PAYLOAD__;
  const SVG_NS = "http://www.w3.org/2000/svg";
  const columns = document.getElementById("columns");
  const overlay = document.getElementById("overlay");

  const peopleEls = new Map();
  document.querySelectorAll('.col[data-table="people"] .row').forEach((r) => {
    peopleEls.set(r.dataset.id, r);
  });
  const placeEls = new Map();
  document.querySelectorAll('.col[data-table="places"] .row').forEach((r) => {
    placeEls.set(r.dataset.id, r);
  });

  // Neutral (gray) edges are painted first so accent edges sit on top.
  const lines = [];
  const neutralFrag = document.createDocumentFragment();
  const accentFrag = document.createDocumentFragment();
  for (const e of DATA.edges || []) {
    const ea = peopleEls.get(e.a_id);
    const eb = placeEls.get(e.b_id);
    if (!ea || !eb) continue;
    const path = document.createElementNS(SVG_NS, "path");
    const zoneCls = e.zone === "industrial" ? "zone-industrial" : "zone-tourism";
    path.setAttribute("class", e.style + " " + zoneCls);
    if (e.style === "e-planned-only") {
      neutralFrag.appendChild(path);
    } else {
      accentFrag.appendChild(path);
    }
    lines.push({ path, a: ea, b: eb });
  }
  overlay.appendChild(neutralFrag);
  overlay.appendChild(accentFrag);

  function layout() {
    const cRect = columns.getBoundingClientRect();
    overlay.setAttribute("width", cRect.width);
    overlay.setAttribute("height", cRect.height);
    overlay.setAttribute("viewBox", "0 0 " + cRect.width + " " + cRect.height);

    const peopleCol = columns.querySelector('.col[data-table="people"]');
    const placesCol = columns.querySelector('.col[data-table="places"]');
    const pRect  = peopleCol.getBoundingClientRect();
    const plRect = placesCol.getBoundingClientRect();
    const xPeopleRight = pRect.right  - cRect.left;
    const xPlacesLeft  = plRect.left  - cRect.left;

    for (const item of lines) {
      const ra = item.a.getBoundingClientRect();
      const rb = item.b.getBoundingClientRect();
      const ya = (ra.top - cRect.top) + ra.height / 2;
      const yb = (rb.top - cRect.top) + rb.height / 2;
      const x1 = xPeopleRight - 2;
      const x2 = xPlacesLeft  + 2;
      const cp = (x2 - x1) * 0.45;
      const d = "M " + x1 + "," + ya +
                " C " + (x1 + cp) + "," + ya +
                " " + (x2 - cp) + "," + yb +
                " " + x2 + "," + yb;
      item.path.setAttribute("d", d);
    }
  }

  requestAnimationFrame(() => {
    layout();
    requestAnimationFrame(layout);
    setTimeout(layout, 120);
    setTimeout(layout, 480);
  });
  window.addEventListener("resize", () => requestAnimationFrame(layout));
  if (window.ResizeObserver) {
    new ResizeObserver(() => requestAnimationFrame(layout)).observe(columns);
  }
})();
</script>
</body>
</html>"""

    widget_html = (
        HTML_TEMPLATE
        .replace("__IND__", INDUSTRIAL)
        .replace("__IND_LIGHT__", INDUSTRIAL_LIGHT)
        .replace("__IND_INK__", INDUSTRIAL_INK)
        .replace("__TOUR__", TOURISM)
        .replace("__TOUR_LIGHT__", TOURISM_LIGHT)
        .replace("__TOUR_INK__", TOURISM_INK)
        .replace("__NEU_DARK__", NEUTRAL_DARK)
        .replace("__NEU_LIGHT__", NEUTRAL_LIGHT)
        .replace("__IND_BAR__",  INDUSTRIAL_BAR_FILL)
        .replace("__TOUR_BAR__", TOURISM_BAR_FILL)
        .replace("__IND_MID__",  INDUSTRIAL_MID)
        .replace("__TOUR_MID__", TOURISM_MID)
        .replace("__TITLE__", title_html)
        .replace("__LEGEND__", legend_html)
        .replace("__PEOPLE__", people_html)
        .replace("__PLACES__", places_html)
        .replace("__BULLET__", bullet_html)
        .replace("__FOOTNOTE__", footnote_html)
        .replace("__PAYLOAD__", payload)
    )

    # A4 landscape at 96 dpi: 1122 x 794 CSS px.
    iframe_w = 1122
    iframe_h = 794 + 24

    iframe_html = (
        '<style>'
        '  .explainer-wrap {'
        '    display: flex; justify-content: center;'
        '    box-sizing: border-box;'
        '    position: relative;'
        '    width: 100vw; min-width: 100vw; max-width: 100vw;'
        '    left: 50%; right: 50%;'
        '    margin-left: -50vw; margin-right: -50vw;'
        '    flex-shrink: 0;'
        '    overflow-x: auto;'
        '  }'
        '  :has(> .explainer-wrap),'
        '  :has(> * > .explainer-wrap),'
        '  :has(> * > * > .explainer-wrap),'
        '  :has(> * > * > * > .explainer-wrap) {'
        '    max-width: none !important;'
        '  }'
        '</style>'
        '<div class="explainer-wrap">'
        '<iframe srcdoc="'
        + _html.escape(widget_html, quote=True)
        + f'" style="display:block; width:{iframe_w}px; min-width:{iframe_w}px;'
          f' height:{iframe_h}px; border:1px solid #cbd5e1; border-radius:4px;'
          f' background:#f1f5f9;"></iframe>'
        '</div>'
    )

    widget = mo.Html(iframe_html)

    return (widget,)


@app.cell
def _(widget):
    widget
    return


if __name__ == "__main__":
    app.run()
