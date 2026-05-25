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
    topics_df = read_csv(JD + "topics.csv")
    plan_topics_df = read_csv(JD + "plan_topics.csv")
    ppp_df = read_csv(JD + "plan_people_participations.csv")
    travel_links_df = read_csv(JD + "travel_links.csv")
    trip_people_df = read_csv(JD + "trip_people.csv")
    trip_places_df = read_csv(JD + "trip_places.csv")

    # plan_id -> topic_id
    plan_to_topic = {
        str(r["plan_id"]).strip(): str(r["topic_id"]).strip()
        for _, r in plan_topics_df.iterrows()
    }
    # topic_id -> short_topic title-cased
    topic_label = {
        str(r["topic_id"]).strip(): str(r["short_topic"]).strip().replace("_", " ").title()
        for _, r in topics_df.iterrows()
    }

    # ------------------------------------------------------------ design tokens
    INDUSTRIAL_DARK = "#b45309"
    INDUSTRIAL_LIGHT = "#fde68a"
    INDUSTRIAL_INK = "#78350f"
    TOURISM_DARK = "#0d9488"
    TOURISM_LIGHT = "#99f6e4"
    TOURISM_INK = "#134e4a"
    NEUTRAL_DARK = "#94a3b8"
    NEUTRAL_LIGHT = "#e2e8f0"
    TICK_COLOR = "#1e293b"
    INK = "#0f172a"
    SLATE = "#475569"
    MUTED = "#64748b"

    ANCHOR_PLACE_INDUSTRIAL = "3753651731"  # High Seas Fishing Inc.
    ANCHOR_PLACE_TOURISM = "37828755"  # Dock & Roll Hall of Fame
    ANCHOR_PLACES = {ANCHOR_PLACE_INDUSTRIAL, ANCHOR_PLACE_TOURISM}
    ANCHOR_PLANS = {
        "deep_fishing_dock_Travel_High_Seas_Fishing_Inc",
        "seafood_festival_Travel_Dock_Roll_Hall_of_Fame",
    }

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

    # plan_id -> set of people (from plan_people_participations, restricted to kept plans)
    plan_people: dict[str, set[str]] = defaultdict(set)
    for _, r in ppp_df.iterrows():
        plan_id = str(r["plan_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if plan_id in kept_plan_ids and person_id:
            plan_people[plan_id].add(person_id)

    # plan_id -> destination place_id (one per Travel plan)
    plan_to_place = {
        str(r["plan_id"]).strip(): str(r["place_id"]).strip()
        for _, r in kept_tl.iterrows()
    }

    # planned_per_place[place_id] = set of unique board members planned to visit
    planned_per_place: dict[str, set[str]] = defaultdict(set)
    for plan_id, place_id in plan_to_place.items():
        planned_per_place[place_id] |= plan_people.get(plan_id, set())

    # visited_per_place[place_id] = set of unique board members who visited
    visited_per_place: dict[str, set[str]] = defaultdict(set)
    trip_merged = trip_people_df.merge(trip_places_df, on="trip_id", how="inner")
    for _, r in trip_merged.iterrows():
        place_id = str(r["place_id"]).strip()
        person_id = str(r["people_id"]).strip()
        if place_id in all_zone_place_ids and person_id:
            visited_per_place[place_id].add(person_id)

    # ----------------------------------------------------- people entries
    ROLE_ORDER = {
        "Committee Chair": 0,
        "Vice Chair": 1,
        "Treasurer": 2,
        "Member": 3,
    }

    people_entries = []
    for _, r in people_df.iterrows():
        pid = str(r["people_id"]).strip()
        name = str(r["name"]).strip()
        role = str(r["role"]).strip() or "Member"
        people_entries.append(
            {"id": pid, "label": name, "group": role, "row_class": "row-neutral"}
        )
    people_entries.sort(key=lambda e: (ROLE_ORDER.get(e["group"], 99), e["label"].lower()))

    # ----------------------------------------------------- plan entries
    # Determine each topic's zone (all kept Travel plans within a topic go to a single zone).
    def place_zone(pid: str) -> str:
        rows = zone_places_df[zone_places_df["place_id"] == pid]
        if len(rows) == 0:
            return ""
        return str(rows.iloc[0]["zone"])

    place_name_map = {
        str(r["place_id"]).strip(): str(r["name"]).strip()
        for _, r in named_zone.iterrows()
    }

    topic_zone: dict[str, str] = {}
    topic_has_anchor: dict[str, bool] = {}
    for plan_id in kept_plan_ids:
        topic = plan_to_topic.get(plan_id, "")
        place_id = plan_to_place[plan_id]
        z = place_zone(place_id)
        if z:
            topic_zone[topic] = z
        if plan_id in ANCHOR_PLANS:
            topic_has_anchor[topic] = True

    plan_entries = []
    for plan_id in kept_plan_ids:
        topic_id = plan_to_topic.get(plan_id, "")
        place_id = plan_to_place[plan_id]
        place_name = place_name_map.get(place_id, place_id)
        zone = place_zone(place_id)
        is_anchor = plan_id in ANCHOR_PLANS
        plan_entries.append(
            {
                "id": plan_id,
                "label": "\U0001f697 \u2192 " + place_name,
                "group": topic_id,
                "group_label": topic_label.get(topic_id, topic_id),
                "zone": zone,
                "is_anchor": is_anchor,
                "place_id": place_id,
                "row_class": (
                    "row-industrial-dark"
                    if (is_anchor and zone == "industrial")
                    else "row-tourism-dark"
                    if (is_anchor and zone == "tourism")
                    else "row-neutral"
                ),
            }
        )

    ZONE_ORDER = {"industrial": 0, "tourism": 1}
    plan_entries.sort(
        key=lambda e: (
            ZONE_ORDER.get(e["zone"], 9),
            0 if topic_has_anchor.get(e["group"]) else 1,
            e["group_label"].lower(),
            0 if e["is_anchor"] else 1,
            e["label"].lower(),
        )
    )

    # ----------------------------------------------------- place entries
    AGG_INDUSTRIAL_ID = "_agg_industrial"
    AGG_TOURISM_ID = "_agg_tourism"

    def make_named_place_entry(row):
        place_id = str(row["place_id"]).strip()
        zone = str(row["zone"]).strip()
        is_anchor = place_id in ANCHOR_PLACES
        v = len(visited_per_place.get(place_id, set()))
        p = len(planned_per_place.get(place_id, set()))
        return {
            "id": place_id,
            "label": str(row["name"]).strip(),
            "group": zone,
            "zone": zone,
            "is_anchor": is_anchor,
            "is_aggregate": False,
            "visited": v,
            "planned": p,
            "visitors": visited_per_place.get(place_id, set()),
            "planners": planned_per_place.get(place_id, set()),
            "row_class": (
                "row-industrial-dark"
                if (is_anchor and zone == "industrial")
                else "row-tourism-dark"
                if (is_anchor and zone == "tourism")
                else "row-industrial-light"
                if zone == "industrial"
                else "row-tourism-light"
            ),
        }

    place_entries: list[dict] = []
    for _, row in named_zone.iterrows():
        place_entries.append(make_named_place_entry(row))

    # Aggregate rows for unnamed places, one per zone (if any exist).
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
        place_entries.append(
            {
                "id": agg_id,
                "label": f"({len(member_pids)} unnamed {zone} sites)",
                "group": zone,
                "zone": zone,
                "is_anchor": False,
                "is_aggregate": True,
                "visited": len(union_visitors),
                "planned": len(union_planners),
                "visitors": union_visitors,
                "planners": union_planners,
                "member_place_ids": member_pids,
                "row_class": "row-aggregate",
            }
        )

    # Sort places: zone (industrial first), visited desc, anchor first, planned desc, name.
    place_entries.sort(
        key=lambda e: (
            ZONE_ORDER.get(e["zone"], 9),
            -e["visited"],
            0 if e["is_anchor"] else 1,
            -e["planned"],
            e["label"].lower(),
        )
    )

    # Map unnamed place_id -> aggregate id (so trips to unnamed sites route to the
    # aggregate row when drawing people<->places edges).
    unnamed_to_agg: dict[str, str] = {}
    for ent in place_entries:
        if ent.get("is_aggregate"):
            for spid in ent.get("member_place_ids", set()):
                unnamed_to_agg[spid] = ent["id"]

    # ----------------------------------------------------- edges
    place_index = {e["id"]: e for e in place_entries}
    plan_index = {e["id"]: e for e in plan_entries}

    edges: list[dict] = []

    # plans <-> people (only kept Travel plans)
    for plan_id, people_set in plan_people.items():
        plan = plan_index.get(plan_id)
        if not plan:
            continue
        for person_id in people_set:
            anchor = plan["is_anchor"]
            edges.append(
                {
                    "a_table": "people",
                    "a_id": person_id,
                    "b_table": "plans",
                    "b_id": plan_id,
                    "anchor": anchor,
                    "zone": plan["zone"],
                }
            )

    # plans <-> places (one per kept Travel plan)
    for plan_id, place_id in plan_to_place.items():
        plan = plan_index.get(plan_id)
        place = place_index.get(place_id)
        if not plan or not place:
            continue
        anchor = plan["is_anchor"] and place["is_anchor"]
        edges.append(
            {
                "a_table": "plans",
                "a_id": plan_id,
                "b_table": "places",
                "b_id": place_id,
                "anchor": anchor,
                "zone": plan["zone"],
            }
        )

    # people <-> places (trips). Route unnamed place visits to the aggregate row.
    seen_pp_edges: set[tuple[str, str]] = set()
    for (place_id, person_set) in visited_per_place.items():
        target_id = place_id
        if place_id not in named_place_ids:
            target_id = unnamed_to_agg.get(place_id)
            if target_id is None:
                continue
        target_place = place_index.get(target_id)
        if not target_place:
            continue
        for person_id in person_set:
            key = (person_id, target_id)
            if key in seen_pp_edges:
                continue
            seen_pp_edges.add(key)
            edges.append(
                {
                    "a_table": "people",
                    "a_id": person_id,
                    "b_table": "places",
                    "b_id": target_id,
                    "anchor": False,  # spec: only plan-related edges get anchor styling
                    "zone": target_place["zone"],
                }
            )

    # ----------------------------------------------------- payload for JS
    import json as _json

    payload = _json.dumps(
        {
            "people": [
                {"id": e["id"], "label": e["label"], "group": e["group"]}
                for e in people_entries
            ],
            "plans": [
                {
                    "id": e["id"],
                    "label": e["label"],
                    "group": e["group"],
                    "group_label": e["group_label"],
                    "zone": e["zone"],
                    "row_class": e["row_class"],
                }
                for e in plan_entries
            ],
            "places": [
                {
                    "id": e["id"],
                    "label": e["label"],
                    "group": e["group"],
                    "zone": e["zone"],
                    "row_class": e["row_class"],
                    "visited": e["visited"],
                    "planned": e["planned"],
                    "is_anchor": e["is_anchor"],
                    "is_aggregate": e["is_aggregate"],
                }
                for e in place_entries
            ],
            "edges": edges,
            "industrial_dark": INDUSTRIAL_DARK,
            "industrial_light": INDUSTRIAL_LIGHT,
            "tourism_dark": TOURISM_DARK,
            "tourism_light": TOURISM_LIGHT,
            "neutral_dark": NEUTRAL_DARK,
            "neutral_light": NEUTRAL_LIGHT,
            "tick_color": TICK_COLOR,
            "scale_max": SCALE_MAX,
        },
        ensure_ascii=False,
    )

    # ----------------------------------------------------- bullet SVG helper
    def bullet_svg(entry: dict) -> str:
        """Render the inline bullet chart for a single place row."""
        visited = entry["visited"]
        planned = entry["planned"]
        is_anchor = entry["is_anchor"]
        is_aggregate = entry["is_aggregate"]
        zone = entry["zone"]

        # Geometry
        width = 248
        height = 22
        pad_left = 6
        pad_right = 6
        unit = 28  # px per board member on the 0-6 scale
        track_w = unit * SCALE_MAX
        bar_h = 10
        track_y = height // 2 - bar_h // 2
        tick_extra = 5

        # Bar color
        if is_aggregate:
            bar_fill = NEUTRAL_DARK
        elif zone == "industrial":
            bar_fill = INDUSTRIAL_DARK if is_anchor else INDUSTRIAL_LIGHT
        else:
            bar_fill = TOURISM_DARK if is_anchor else TOURISM_LIGHT

        bar_w = max(0, unit * visited)
        tick_x = pad_left + unit * planned

        # Label rules: anchor OR |v - p| >= 3
        show_label = is_anchor or abs(visited - planned) >= 3
        # Always show the aggregate label if it satisfies the diff rule (diff=3 for industrial agg)
        # already handled.

        parts = [
            f'<svg class="bullet" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            f'  <rect x="{pad_left}" y="{track_y}" width="{track_w}" height="{bar_h}" rx="2" fill="{NEUTRAL_LIGHT}" />',
        ]
        if bar_w > 0:
            parts.append(
                f'  <rect x="{pad_left}" y="{track_y}" width="{bar_w}" height="{bar_h}" rx="2" fill="{bar_fill}" />'
            )
        parts.append(
            f'  <line x1="{tick_x}" x2="{tick_x}" y1="{track_y - tick_extra}" y2="{track_y + bar_h + tick_extra}" stroke="{TICK_COLOR}" stroke-width="2" stroke-linecap="round" />'
        )
        if show_label:
            label_x = pad_left + track_w + 8
            parts.append(
                f'  <text x="{label_x}" y="{height / 2 + 3.5}" font-size="10" font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif" fill="{INK}" font-weight="600">'
                f'<tspan>{visited}</tspan><tspan dx="3" fill="{MUTED}">\u00b7</tspan><tspan dx="3" fill="{MUTED}" font-weight="400">{planned}</tspan>'
                "</text>"
            )
        parts.append("</svg>")
        return "\n".join(parts)

    # Pre-render bullet SVG strings per row (Python-side so it stays in one place).
    place_bullet_html = {e["id"]: bullet_svg(e) for e in place_entries}

    # ----------------------------------------------------- HTML rendering
    def column_html(entries: list[dict], render_row) -> str:
        """Render a column with grouped rows."""
        out: list[str] = ['<div class="list">']
        current_group = None
        for e in entries:
            if e["group"] != current_group:
                if current_group is not None:
                    out.append("</div>")  # close previous .group
                current_group = e["group"]
                group_text = e.get("group_label", current_group)
                out.append('<div class="group">')
                out.append(
                    f'<div class="group-header" data-zone="{e.get("zone", "")}">{_html.escape(str(group_text))}</div>'
                )
            out.append(render_row(e))
        if current_group is not None:
            out.append("</div>")
        out.append("</div>")
        return "\n".join(out)

    def people_row(e: dict) -> str:
        return (
            f'<div class="row {e["row_class"]}" '
            f'data-table="people" data-id="{_html.escape(e["id"])}">'
            f'{_html.escape(e["label"])}'
            "</div>"
        )

    def plan_row(e: dict) -> str:
        return (
            f'<div class="row {e["row_class"]}" data-zone="{e["zone"]}" '
            f'data-anchor="{"1" if e["is_anchor"] else "0"}" '
            f'data-table="plans" data-id="{_html.escape(e["id"])}">'
            f'{_html.escape(e["label"])}'
            "</div>"
        )

    def place_row(e: dict) -> str:
        return (
            f'<div class="row {e["row_class"]}" data-zone="{e["zone"]}" '
            f'data-anchor="{"1" if e["is_anchor"] else "0"}" '
            f'data-table="places" data-id="{_html.escape(e["id"])}">'
            f'{_html.escape(e["label"])}'
            "</div>"
        )

    def bullet_row(e: dict) -> str:
        return (
            f'<div class="row bullet-row" data-zone="{e["zone"]}" '
            f'data-table="bullet" data-id="{_html.escape(e["id"])}">'
            f'{place_bullet_html[e["id"]]}'
            "</div>"
        )

    # Plans column groups carry a `group_label` (long-form) — adapt entry tuples.
    plan_entries_for_render = [
        dict(e, group_label=e.get("group_label", e["group"])) for e in plan_entries
    ]
    place_entries_for_render = [
        dict(e, group_label="Industrial sites" if e["zone"] == "industrial" else "Tourism sites")
        for e in place_entries
    ]

    people_html = column_html(people_entries, people_row)
    plans_html = column_html(plan_entries_for_render, plan_row)
    places_html = column_html(place_entries_for_render, place_row)
    bullet_html = column_html(place_entries_for_render, bullet_row)

    title_html = (
        '<span class="t-ind">Fishing locations</span> are often planned but unvisited. '
        'In contrast, some <span class="t-tour">tourism locations</span> are often '
        'visited outside of plans.'
    )
    subtitle_html = (
        '<span class="t-ind">High Seas Fishing</span> and '
        '<span class="t-tour">Dock &amp; Roll Hall of Fame</span> were each planned '
        'to be visited by 1 board member. In reality, the former remained unvisited, '
        'while the latter was visited by all 6.'
    )
    footnote_html = (
        "Industrial &amp; Tourism zones only \u00b7 Unnamed sites aggregated \u00b7 "
        "Source: journalist field notes."
    )
    legend_html = (
        '<span class="lg-bar"></span>visited&nbsp;&nbsp;'
        '<span class="lg-tick"></span>planned'
        '<span class="lg-sep">\u2009\u2009(scale 0\u2013'
        f'{SCALE_MAX} board members)</span>'
    )

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  :root {
    --industrial-dark: __IND_DARK__;
    --industrial-light: __IND_LIGHT__;
    --industrial-ink: __IND_INK__;
    --tourism-dark: __TOUR_DARK__;
    --tourism-light: __TOUR_LIGHT__;
    --tourism-ink: __TOUR_INK__;
    --neutral-dark: __NEU_DARK__;
    --neutral-light: __NEU_LIGHT__;
    --ink: #0f172a;
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
    grid-template-rows: auto auto auto 1fr auto;
    grid-template-columns: 1fr;
    row-gap: 6px;
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
  .title .t-ind  { color: var(--industrial-dark); }
  .title .t-tour { color: var(--tourism-dark); }
  .subtitle {
    font-size: 13px;
    line-height: 1.5;
    color: var(--slate);
    margin: 0;
    max-width: 1060px;
  }
  .subtitle .t-ind  { color: var(--industrial-dark); font-weight: 600; }
  .subtitle .t-tour { color: var(--tourism-dark);    font-weight: 600; }
  .legend-row {
    display: flex; align-items: center;
    font-size: 11px; color: var(--muted);
    gap: 0;
    margin-top: 2px;
  }
  .legend-row .lg-bar {
    display: inline-block;
    width: 16px; height: 8px; background: var(--neutral-dark);
    border-radius: 2px; margin-right: 5px;
  }
  .legend-row .lg-tick {
    display: inline-block;
    width: 2px; height: 14px; background: var(--ink);
    margin: 0 5px 0 8px;
    vertical-align: middle;
  }
  .legend-row .lg-sep { color: var(--muted); }
  .columns-wrap {
    position: relative;
    display: grid;
    grid-template-columns: 162px 320px 252px 280px;
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
    /* Center short columns vertically inside the wrap so the figure reads as
       balanced. For the (tall) plans column this is a no-op. The places and
       bullet columns share row structure, so they stay aligned. */
    justify-content: center;
    gap: 6px;
    min-height: 0;
  }
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
    margin-bottom: 3px;
  }
  .group-header[data-zone="industrial"] { color: var(--industrial-ink); border-color: #fef3c7; }
  .group-header[data-zone="tourism"]    { color: var(--tourism-ink);    border-color: #ccfbf1; }
  .row {
    font-size: 11px;
    line-height: 14px;
    /* Locked row height keeps the places column rows pixel-aligned with the
       bullet-chart column rows; box-sizing: border-box is global. */
    height: 22px;
    padding: 4px 6px;
    margin-bottom: 1px;
    border-radius: 3px;
    color: var(--ink);
    background: transparent;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    align-items: center;
  }
  .row.row-industrial-dark {
    background: var(--industrial-dark);
    color: #fff7ed;
    font-weight: 600;
  }
  .row.row-industrial-light {
    background: var(--industrial-light);
    color: var(--industrial-ink);
  }
  .row.row-tourism-dark {
    background: var(--tourism-dark);
    color: #ecfdf5;
    font-weight: 600;
  }
  .row.row-tourism-light {
    background: var(--tourism-light);
    color: var(--tourism-ink);
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
  .col[data-table="plans"] .row {
    white-space: nowrap;
  }
  .overlay {
    position: absolute; top: 0; left: 0;
    width: 100%; height: 100%;
    pointer-events: none;
  }
  .overlay path {
    fill: none;
    stroke-linecap: round;
  }
  .overlay path.e-neutral {
    stroke: var(--neutral-dark);
    stroke-opacity: 0.18;
    stroke-width: 1;
  }
  .overlay path.e-anchor-ind {
    stroke: var(--industrial-dark);
    stroke-opacity: 0.9;
    stroke-width: 2;
  }
  .overlay path.e-anchor-tour {
    stroke: var(--tourism-dark);
    stroke-opacity: 0.9;
    stroke-width: 2;
  }
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
  <p class="subtitle">__SUBTITLE__</p>
  <div class="legend-row">__LEGEND__</div>
  <div class="columns-wrap" id="columns">
    <div class="col" data-table="people">
      <div class="col-head">People &middot; board members</div>
      __PEOPLE__
    </div>
    <div class="col" data-table="plans">
      <div class="col-head">Plans &middot; travel by topic</div>
      __PLANS__
    </div>
    <div class="col" data-table="places">
      <div class="col-head">Destinations &middot; by zone</div>
      __PLACES__
    </div>
    <div class="col" data-table="bullet">
      <div class="col-head">Members planned vs visited</div>
      __BULLET__
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

  // Build a key -> DOM row map for fast lookup during layout.
  const keyOf = (t, id) => t + "\u0000" + id;
  const rowEls = new Map();
  document.querySelectorAll(".col .row").forEach((r) => {
    const t = r.dataset.table;
    const id = r.dataset.id;
    if (t && id) rowEls.set(keyOf(t, id), r);
  });

  const ORDER = { people: 0, plans: 1, places: 2, bullet: 3 };

  // Build edges as SVG <path> elements once.
  const lines = [];
  const frag = document.createDocumentFragment();
  for (const e of DATA.edges || []) {
    // bullet column never participates in edges
    const ka = keyOf(e.a_table, e.a_id);
    const kb = keyOf(e.b_table, e.b_id);
    if (!rowEls.has(ka) || !rowEls.has(kb)) continue;
    const path = document.createElementNS(SVG_NS, "path");
    let cls = "e-neutral";
    if (e.anchor) {
      cls = e.zone === "industrial" ? "e-anchor-ind" : "e-anchor-tour";
    }
    path.setAttribute("class", cls);
    frag.appendChild(path);
    lines.push({
      path,
      a: ka, b: kb,
      aTable: e.a_table, bTable: e.b_table,
      anchor: !!e.anchor,
    });
  }
  // Append anchor edges last so they paint on top of the neutral edges.
  overlay.appendChild(frag);
  for (const item of lines) {
    if (item.anchor) overlay.appendChild(item.path);
  }

  function layout() {
    const cRect = columns.getBoundingClientRect();
    overlay.setAttribute("width", cRect.width);
    overlay.setAttribute("height", cRect.height);
    overlay.setAttribute("viewBox", `0 0 ${cRect.width} ${cRect.height}`);

    // Cache column horizontal extents.
    const colRects = {};
    for (const t of ["people", "plans", "places", "bullet"]) {
      const col = columns.querySelector('.col[data-table="' + t + '"]');
      const cr = col.getBoundingClientRect();
      colRects[t] = { left: cr.left - cRect.left, right: cr.right - cRect.left };
    }

    for (const item of lines) {
      const ea = rowEls.get(item.a);
      const eb = rowEls.get(item.b);
      if (!ea || !eb) {
        item.path.setAttribute("d", "");
        continue;
      }
      const ra = ea.getBoundingClientRect();
      const rb = eb.getBoundingClientRect();
      const ya = (ra.top - cRect.top) + ra.height / 2;
      const yb = (rb.top - cRect.top) + rb.height / 2;
      let leftT = item.aTable, rightT = item.bTable;
      let leftY = ya, rightY = yb;
      if (ORDER[item.aTable] > ORDER[item.bTable]) {
        leftT = item.bTable; rightT = item.aTable;
        leftY = yb; rightY = ya;
      }
      const x1 = colRects[leftT].right - 2;
      const x2 = colRects[rightT].left + 2;
      const cp = (x2 - x1) * 0.45;
      const d = "M " + x1 + "," + leftY +
                " C " + (x1 + cp) + "," + leftY +
                " " + (x2 - cp) + "," + rightY +
                " " + x2 + "," + rightY;
      item.path.setAttribute("d", d);
    }
  }

  // Run layout twice on next frames to account for font/layout settling.
  requestAnimationFrame(() => {
    layout();
    requestAnimationFrame(layout);
    setTimeout(layout, 120);
    setTimeout(layout, 480);
  });
  // Re-layout on any container size change (e.g., browser zoom / window resize).
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
        .replace("__IND_DARK__", INDUSTRIAL_DARK)
        .replace("__IND_LIGHT__", INDUSTRIAL_LIGHT)
        .replace("__IND_INK__", INDUSTRIAL_INK)
        .replace("__TOUR_DARK__", TOURISM_DARK)
        .replace("__TOUR_LIGHT__", TOURISM_LIGHT)
        .replace("__TOUR_INK__", TOURISM_INK)
        .replace("__NEU_DARK__", NEUTRAL_DARK)
        .replace("__NEU_LIGHT__", NEUTRAL_LIGHT)
        .replace("__TITLE__", title_html)
        .replace("__SUBTITLE__", subtitle_html)
        .replace("__LEGEND__", legend_html)
        .replace("__PEOPLE__", people_html)
        .replace("__PLANS__", plans_html)
        .replace("__PLACES__", places_html)
        .replace("__BULLET__", bullet_html)
        .replace("__FOOTNOTE__", footnote_html)
        .replace("__PAYLOAD__", payload)
    )

    # A4 landscape at 96 dpi: 1122 x 794 CSS px. The iframe hosts that page at
    # its exact pixel size; the wrapper escapes marimo's narrow content column
    # so the figure renders at full A4 width even inside a narrow notebook.
    iframe_w = 1122
    iframe_h = 794 + 24  # small allowance for the inner shadow / scrollbar

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
        '  /* Break out of any marimo ancestor that imposes a max-width. */'
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

    # Diagnostic counts table for the notebook (mirrors the validation list in the plan).
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
