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
    INDUSTRIAL = "#2563eb"        # blue-600
    INDUSTRIAL_LIGHT = "#dbeafe"  # blue-100
    INDUSTRIAL_INK = "#1e3a8a"    # blue-900
    INDUSTRIAL_BAR_FILL = "#93c5fd"  # blue-300 (filled bar interior)
    TOURISM = "#ea580c"           # orange-600
    TOURISM_LIGHT = "#ffedd5"     # orange-100
    TOURISM_INK = "#7c2d12"       # orange-900
    TOURISM_BAR_FILL = "#fdba74"  # orange-300

    NEUTRAL_DARK = "#94a3b8"     # slate-400 (gray line for planned-not-visited)
    NEUTRAL_LIGHT = "#e2e8f0"
    AGG_BAR_FILL = "#cbd5e1"     # slate-300
    AGG_BAR_STROKE = "#64748b"   # slate-500
    TICK_COLOR = "#1e293b"
    INK = "#0f172a"
    MUTED = "#64748b"

    ANCHOR_PLACE_INDUSTRIAL = "3753651731"  # High Seas Fishing Inc.
    ANCHOR_PLACE_TOURISM = "37828755"       # Dock & Roll Hall of Fame
    SCALE_MAX = 6  # full board

    # --------------------------------------------------------- restrict scope
    KEPT_ZONES = {"industrial", "tourism"}
    zone_places_df = places_df[places_df["zone"].isin(KEPT_ZONES)].copy()
    named_zone = zone_places_df[zone_places_df["name"] != ""].copy()
    unnamed_zone = zone_places_df[zone_places_df["name"] == ""].copy()

    named_place_ids = set(named_zone["place_id"])
    all_zone_place_ids = set(zone_places_df["place_id"])

    travel_plans = plans_df[plans_df["plan_type"].str.title() == "Travel"].copy()
    travel_plan_ids = set(travel_plans["plan_id"])

    # Travel links restricted to Travel plans whose destination is a NAMED zone place.
    kept_tl = travel_links_df[
        travel_links_df["plan_id"].isin(travel_plan_ids)
        & travel_links_df["place_id"].isin(named_place_ids)
    ].copy()
    kept_plan_ids = set(kept_tl["plan_id"])

    # plan_id -> set of people (from plan_people_participations, restricted to kept plans).
    plan_people: dict[str, set[str]] = defaultdict(set)
    for _, r in ppp_df.iterrows():
        plan_id = str(r["plan_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if plan_id in kept_plan_ids and person_id:
            plan_people[plan_id].add(person_id)

    # plan_id -> destination place_id (one per Travel plan).
    plan_to_place = {
        str(r["plan_id"]).strip(): str(r["place_id"]).strip()
        for _, r in kept_tl.iterrows()
    }

    # planned_per_place[place_id] = set of unique board members planned to visit.
    planned_per_place: dict[str, set[str]] = defaultdict(set)
    for plan_id, place_id in plan_to_place.items():
        planned_per_place[place_id] |= plan_people.get(plan_id, set())

    # visited_per_place[place_id] = set of unique board members who actually visited.
    visited_per_place: dict[str, set[str]] = defaultdict(set)
    trip_merged = trip_people_df.merge(trip_places_df, on="trip_id", how="inner")
    for _, r in trip_merged.iterrows():
        place_id = str(r["place_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if place_id in all_zone_place_ids and person_id:
            visited_per_place[place_id].add(person_id)

    # ----------------------------------------------------- people entries
    # Flat alphabetical list of the 6 board members; role grouping is gone.
    people_entries = []
    for _, r in people_df.iterrows():
        pid = str(r["people_id"]).strip()
        name = str(r["name"]).strip()
        people_entries.append({"id": pid, "label": name})
    people_entries.sort(key=lambda e: e["label"].lower())

    # ----------------------------------------------------- place entries
    AGG_INDUSTRIAL_ID = "_agg_industrial"
    AGG_TOURISM_ID = "_agg_tourism"

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
            "is_anchor": place_id in {ANCHOR_PLACE_INDUSTRIAL, ANCHOR_PLACE_TOURISM},
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

    # Aggregate rows for unnamed places, one per zone (if any unnamed exist).
    for zone, agg_id in (("industrial", AGG_INDUSTRIAL_ID), ("tourism", AGG_TOURISM_ID)):
        unnamed = unnamed_zone[unnamed_zone["zone"] == zone]
        if len(unnamed) == 0:
            continue
        union_visitors: set[str] = set()
        union_planners: set[str] = set()
        member_pids: set[str] = set()
        for pid in unnamed["place_id"]:
            spid = str(pid).strip()
            member_pids.add(spid)
            union_visitors |= visited_per_place.get(spid, set())
            union_planners |= planned_per_place.get(spid, set())
        both = union_planners & union_visitors
        unplanned = union_visitors - union_planners
        place_entries.append(
            {
                "id": agg_id,
                "label": f"({len(member_pids)} unnamed {zone} sites)",
                "group": zone,
                "zone": zone,
                "is_anchor": False,
                "is_aggregate": True,
                "planned": len(union_planners),
                "visited": len(union_visitors),
                "both": len(both),
                "unplanned_visited": len(unplanned),
                "planners": union_planners,
                "visitors": union_visitors,
                "member_place_ids": member_pids,
                "row_class": "row-aggregate",
            }
        )

    # Sort places: zone (industrial first), visited desc, anchor first, planned desc, name.
    ZONE_ORDER = {"industrial": 0, "tourism": 1}
    place_entries.sort(
        key=lambda e: (
            ZONE_ORDER.get(e["zone"], 9),
            -e["visited"],
            0 if e["is_anchor"] else 1,
            -e["planned"],
            e["label"].lower(),
        )
    )

    # Map unnamed place_id -> aggregate id so trips to unnamed sites route to the
    # aggregate row when drawing person<->place edges.
    unnamed_to_agg: dict[str, str] = {}
    for ent in place_entries:
        if ent.get("is_aggregate"):
            for spid in ent.get("member_place_ids", set()):
                unnamed_to_agg[spid] = ent["id"]

    # ----------------------------------------------------- edges (3 styles)
    # One edge per (person, place_or_agg) with style:
    #   "e-both"          planned and visited        (solid colored)
    #   "e-planned-only"  planned, not visited       (gray solid)
    #   "e-unplanned"     not planned, visited       (colored dotted)
    place_index = {e["id"]: e for e in place_entries}
    valid_people = {e["id"] for e in people_entries}

    person_place_state: dict[tuple[str, str], dict[str, bool]] = defaultdict(
        lambda: {"planned": False, "visited": False}
    )

    # Planned: any person rostered on a Travel plan whose destination is the place
    # (or, for unnamed destinations, the corresponding aggregate row).
    for place_id, planners in planned_per_place.items():
        target = place_id if place_id in named_place_ids else unnamed_to_agg.get(place_id)
        if not target or target not in place_index:
            continue
        for person_id in planners:
            if person_id in valid_people:
                person_place_state[(person_id, target)]["planned"] = True

    # Visited: from trip_people * trip_places, routed through unnamed_to_agg.
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

    # ----------------------------------------------------- payload for JS
    import json as _json

    payload = _json.dumps(
        {
            "edges": edges,
            "anchor_industrial": ANCHOR_PLACE_INDUSTRIAL,
            "anchor_tourism": ANCHOR_PLACE_TOURISM,
        },
        ensure_ascii=False,
    )

    # ----------------------------------------------------- bullet SVG helper
    def bullet_svg(entry: dict) -> str:
        """Two-segment bullet bar: filled with solid outline for planned visits,
        filled with dotted outline for unplanned visits, plus a vertical tick at
        the total planned count. No gray track behind the bar."""
        both = entry["both"]
        unplanned = entry["unplanned_visited"]
        planned_total = entry["planned"]
        visited_total = both + unplanned
        is_aggregate = entry["is_aggregate"]
        zone = entry["zone"]

        # Geometry sized to the ~280 px Bullet column.
        width = 270
        height = 30
        pad_left = 6
        unit = 36
        track_w = unit * SCALE_MAX
        bar_h = 14
        bar_y = (height - bar_h) // 2
        tick_extra = 6

        if is_aggregate:
            fill = AGG_BAR_FILL
            stroke = AGG_BAR_STROKE
        elif zone == "industrial":
            fill = INDUSTRIAL_BAR_FILL
            stroke = INDUSTRIAL
        else:
            fill = TOURISM_BAR_FILL
            stroke = TOURISM

        x_solid = pad_left
        w_solid = unit * both
        x_dotted = pad_left + w_solid
        w_dotted = unit * unplanned
        tick_x = pad_left + unit * planned_total

        parts = [
            f'<svg class="bullet" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" preserveAspectRatio="xMinYMid meet">'
        ]
        # Solid-outline filled rect (planned and visited).
        if w_solid > 0:
            parts.append(
                f'<rect x="{x_solid}" y="{bar_y}" width="{w_solid}" height="{bar_h}" '
                f'fill="{fill}" stroke="{stroke}" stroke-width="1.5" rx="1.5" />'
            )
        # Dotted-outline filled rect (visited but not planned).
        if w_dotted > 0:
            parts.append(
                f'<rect x="{x_dotted}" y="{bar_y}" width="{w_dotted}" height="{bar_h}" '
                f'fill="{fill}" stroke="{stroke}" stroke-width="1.5" '
                f'stroke-dasharray="2.5 2" rx="1.5" />'
            )
        # Vertical tick at the planned-total position.
        parts.append(
            f'<line x1="{tick_x}" x2="{tick_x}" y1="{bar_y - tick_extra}" '
            f'y2="{bar_y + bar_h + tick_extra}" stroke="{TICK_COLOR}" '
            f'stroke-width="2" stroke-linecap="round" />'
        )

        # Inline count label: show when the visited/planned gap is large enough
        # to be worth annotating numerically (also covers the aggregate row).
        show_label = abs(visited_total - planned_total) >= 3
        if show_label:
            label_x = pad_left + track_w + 8
            parts.append(
                f'<text x="{label_x}" y="{height / 2 + 3.5}" font-size="11" '
                f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif" '
                f'fill="{INK}" font-weight="600">'
                f'<tspan>{visited_total}</tspan>'
                f'<tspan dx="3" fill="{MUTED}">\u00b7</tspan>'
                f'<tspan dx="3" fill="{MUTED}" font-weight="400">{planned_total}</tspan>'
                "</text>"
            )
        parts.append("</svg>")
        return "\n".join(parts)

    place_bullet_html = {e["id"]: bullet_svg(e) for e in place_entries}

    # ----------------------------------------------------- column rendering
    def grouped_column_html(entries: list[dict], render_row, group_label_of) -> str:
        """Render a column with grouped rows (places, bullet)."""
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
                    f'{_html.escape(group_label_of(e))}</div>'
                )
            out.append(render_row(e))
        if current_group is not None:
            out.append("</div>")
        out.append("</div>")
        return "\n".join(out)

    def flat_people_html(entries: list[dict]) -> str:
        """Hand-rolled flat list for the People column (no group headers)."""
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
        return "Industrial sites" if e["zone"] == "industrial" else "Tourism sites"

    people_html = flat_people_html(people_entries)
    places_html = grouped_column_html(place_entries, place_row_html, zone_group_label)
    bullet_html = grouped_column_html(place_entries, bullet_row_html, zone_group_label)

    # Annotation column: two absolutely-positioned callouts that the JS layout
    # pass anchors vertically to their target place row.
    annot_entries = [
        {
            "target": ANCHOR_PLACE_INDUSTRIAL,
            "css_class": "annot annot-ind",
            "text": (
                "High Seas Fishing were planned to be visited by 1 board member "
                "but remain unvisited."
            ),
        },
        {
            "target": ANCHOR_PLACE_TOURISM,
            "css_class": "annot annot-tour",
            "text": (
                "Dock & Roll Hall of Fame were planned to be visited by only 1 "
                "board member, yet visited by all 6 members."
            ),
        },
    ]
    annot_parts = ['<div class="annot-canvas" id="annot-canvas">']
    for a in annot_entries:
        annot_parts.append(
            f'<div class="{a["css_class"]}" data-target="{_html.escape(a["target"])}">'
            f'{_html.escape(a["text"])}</div>'
        )
    annot_parts.append("</div>")
    annotation_html = "\n".join(annot_parts)

    # ----------------------------------------------------- legend (inline SVGs)
    def lg_swatch_solid_colored() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<line x1="0" x2="14" y1="5" y2="5" stroke="{INDUSTRIAL}" stroke-width="2.2" stroke-linecap="round"/>'
            f'<line x1="14" x2="28" y1="5" y2="5" stroke="{TOURISM}" stroke-width="2.2" stroke-linecap="round"/>'
            "</svg>"
        )

    def lg_swatch_dotted_colored() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<line x1="0" x2="14" y1="5" y2="5" stroke="{INDUSTRIAL}" stroke-width="2.2" stroke-linecap="round" stroke-dasharray="3 2.5"/>'
            f'<line x1="14" x2="28" y1="5" y2="5" stroke="{TOURISM}" stroke-width="2.2" stroke-linecap="round" stroke-dasharray="3 2.5"/>'
            "</svg>"
        )

    def lg_swatch_gray() -> str:
        return (
            '<svg class="lg-svg" width="28" height="10" viewBox="0 0 28 10">'
            f'<line x1="0" x2="28" y1="5" y2="5" stroke="{NEUTRAL_DARK}" stroke-width="1.6" stroke-linecap="round"/>'
            "</svg>"
        )

    def lg_swatch_tick() -> str:
        return (
            '<svg class="lg-svg" width="14" height="14" viewBox="0 0 14 14">'
            f'<line x1="7" x2="7" y1="0" y2="14" stroke="{TICK_COLOR}" stroke-width="2" stroke-linecap="round"/>'
            "</svg>"
        )

    legend_html = (
        '<span class="lg-item">' + lg_swatch_solid_colored() + '<span class="lg-label">Planned and visited</span></span>'
        '<span class="lg-item">' + lg_swatch_gray() + '<span class="lg-label">Planned, not visited</span></span>'
        '<span class="lg-item">' + lg_swatch_dotted_colored() + '<span class="lg-label">Not planned, visited</span></span>'
        '<span class="lg-item">' + lg_swatch_tick() + '<span class="lg-label">Planned count</span></span>'
    )

    title_html = (
        '<span class="t-ind">Fishing locations</span> are often planned but unvisited. '
        'In contrast, some <span class="t-tour">tourism locations</span> are often '
        'visited outside of plans.'
    )
    footnote_html = (
        "Industrial &amp; Tourism zones only \u00b7 Unnamed sites aggregated \u00b7 "
        "Source: journalist field notes."
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
    grid-template-columns: 150px 210px 280px 364px;
    gap: 24px;
    align-items: stretch;
    min-height: 0;
    padding-top: 4px;
  }
  .col {
    display: flex; flex-direction: column;
    min-height: 0;
  }
  .col-head {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--muted);
    padding: 0 2px 4px 2px;
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 6px;
  }
  .list {
    flex: 1 1 auto;
    display: flex; flex-direction: column;
    /* Short columns (People, Places, Bullet) center vertically; the tallest
       column drives the actual height, others sit balanced around the middle. */
    justify-content: center;
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
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 4px;
  }
  .group-header[data-zone="industrial"] { color: var(--ind-ink);  border-color: var(--ind-light); }
  .group-header[data-zone="tourism"]    { color: var(--tour-ink); border-color: var(--tour-light); }
  .row {
    font-size: 12px;
    line-height: 16px;
    /* Fixed row height keeps the places column rows pixel-aligned with the
       bullet-chart column rows; box-sizing: border-box is global. */
    height: 30px;
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
    background: var(--ind-light);
    color: var(--ind-ink);
  }
  .row.row-tourism {
    background: var(--tour-light);
    color: var(--tour-ink);
  }
  .row.row-aggregate {
    background: #f1f5f9;
    color: var(--muted);
    font-style: italic;
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
  /* Annotation column: a relatively-positioned canvas that the JS pass anchors
     two absolutely-positioned callouts inside. */
  .annot-col { position: relative; }
  .annot-canvas {
    position: relative; flex: 1 1 auto; min-height: 0;
  }
  .annot {
    position: absolute;
    left: 4px; right: 4px;
    padding: 7px 12px;
    border-left: 3px solid;
    border-radius: 4px;
    font-size: 11.5px;
    line-height: 1.45;
    font-style: italic;
    background: #ffffff;
    box-shadow: 0 1px 4px rgba(15, 23, 42, 0.08);
    /* Vertical alignment is set inline by JS once row positions are known. */
    transform: translateY(-50%);
  }
  .annot.annot-ind  { border-left-color: var(--ind);  color: var(--ind-ink);  background: var(--ind-light); }
  .annot.annot-tour { border-left-color: var(--tour); color: var(--tour-ink); background: var(--tour-light); }
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
    stroke-opacity: 0.9;
    stroke-width: 2;
    stroke-dasharray: 3.5 3;
  }
  .overlay path.e-unplanned.zone-industrial { stroke: var(--ind); }
  .overlay path.e-unplanned.zone-tourism    { stroke: var(--tour); }
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
    .annot { box-shadow: none; }
  }
</style>
</head>
<body>
<div class="page" id="page">
  <h1 class="title">__TITLE__</h1>
  <div class="legend-row">__LEGEND__</div>
  <div class="columns-wrap" id="columns">
    <div class="col" data-table="people">
      <div class="col-head">People &middot; board members</div>
      __PEOPLE__
    </div>
    <div class="col" data-table="places">
      <div class="col-head">Destinations &middot; by zone</div>
      __PLACES__
    </div>
    <div class="col" data-table="bullet">
      <div class="col-head">Members planned vs visited</div>
      __BULLET__
    </div>
    <div class="col annot-col" data-table="annot">
      <div class="col-head">Insight</div>
      __ANNOT__
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

  // Build (table, id) -> DOM row map for fast lookup.
  const keyOf = (t, id) => t + "\u0000" + id;
  const rowEls = new Map();
  document.querySelectorAll(".col .row").forEach((r) => {
    const t = r.dataset.table;
    const id = r.dataset.id;
    if (t && id) rowEls.set(keyOf(t, id), r);
  });

  // Person endpoint lookup (by id, since people column rows are unique).
  const peopleEls = new Map();
  document.querySelectorAll('.col[data-table="people"] .row').forEach((r) => {
    peopleEls.set(r.dataset.id, r);
  });
  const placeEls = new Map();
  document.querySelectorAll('.col[data-table="places"] .row').forEach((r) => {
    placeEls.set(r.dataset.id, r);
  });

  // Build <path> elements for each edge once, classed by style + zone.
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
  // Paint neutral (gray) edges first so accent edges sit on top.
  overlay.appendChild(neutralFrag);
  overlay.appendChild(accentFrag);

  function layout() {
    const cRect = columns.getBoundingClientRect();
    overlay.setAttribute("width", cRect.width);
    overlay.setAttribute("height", cRect.height);
    overlay.setAttribute("viewBox", "0 0 " + cRect.width + " " + cRect.height);

    const peopleCol = columns.querySelector('.col[data-table="people"]');
    const placesCol = columns.querySelector('.col[data-table="places"]');
    const pRect = peopleCol.getBoundingClientRect();
    const plRect = placesCol.getBoundingClientRect();
    const xPeopleRight = pRect.right - cRect.left;
    const xPlacesLeft  = plRect.left  - cRect.left;

    for (const item of lines) {
      const ra = item.a.getBoundingClientRect();
      const rb = item.b.getBoundingClientRect();
      const ya = (ra.top - cRect.top) + ra.height / 2;
      const yb = (rb.top - cRect.top) + rb.height / 2;
      const x1 = xPeopleRight - 2;
      const x2 = xPlacesLeft + 2;
      const cp = (x2 - x1) * 0.45;
      const d = "M " + x1 + "," + ya +
                " C " + (x1 + cp) + "," + ya +
                " " + (x2 - cp) + "," + yb +
                " " + x2 + "," + yb;
      item.path.setAttribute("d", d);
    }

    // Position the annotation callouts vertically against their anchor rows.
    const annotCanvas = document.getElementById("annot-canvas");
    if (annotCanvas) {
      const canvasRect = annotCanvas.getBoundingClientRect();
      annotCanvas.querySelectorAll(".annot").forEach((el) => {
        const target = el.dataset.target;
        const row = placeEls.get(target);
        if (!row) return;
        const rr = row.getBoundingClientRect();
        const y = (rr.top - canvasRect.top) + rr.height / 2;
        el.style.top = y + "px";
      });
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
        .replace("__TITLE__", title_html)
        .replace("__LEGEND__", legend_html)
        .replace("__PEOPLE__", people_html)
        .replace("__PLACES__", places_html)
        .replace("__BULLET__", bullet_html)
        .replace("__ANNOT__", annotation_html)
        .replace("__FOOTNOTE__", footnote_html)
        .replace("__PAYLOAD__", payload)
    )

    # A4 landscape at 96 dpi: 1122 x 794 CSS px. The iframe hosts that page at
    # its exact pixel size; the wrapper escapes marimo's narrow content column
    # so the figure renders at full A4 width even inside a narrow notebook.
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
        + f'" style="display:block; width:{iframe_w}px; min-width:{iframe_w}px; height:{iframe_h}px; border:1px solid #cbd5e1; border-radius:4px; background:#f1f5f9;"></iframe>'
        '</div>'
    )

    widget = mo.Html(iframe_html)

    # Diagnostic counts table for the notebook (mirrors the validation list).
    counts_rows = []
    for e in place_entries:
        counts_rows.append(
            {
                "zone": e["zone"],
                "place": e["label"],
                "anchor": e["is_anchor"],
                "aggregate": e["is_aggregate"],
                "planned": e["planned"],
                "visited": e["visited"],
                "both": e["both"],
                "unplanned_visited": e["unplanned_visited"],
            }
        )
    counts_df = pd.DataFrame(counts_rows)

    return (widget, counts_df)


@app.cell
def _(widget):
    widget
    return


@app.cell
def _(counts_df):
    counts_df
    return


if __name__ == "__main__":
    app.run()
