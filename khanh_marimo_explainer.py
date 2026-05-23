"""Explanatory figure: planned vs. visited contrast for industrial & tourism places.

A static, A4-landscape variant of `khanh_marimo.py` focused on the rhetorical
contrast between board-member visits to industrial versus tourism places, using
journalist-collected data only.

To export the chart as PDF for inclusion in a paper:
  1. Run this marimo notebook.
  2. Right-click the embedded iframe -> "This Frame" -> "Open Frame in New Tab".
  3. In the new tab, Cmd/Ctrl-P -> Save as PDF -> A4 landscape, margins None.
  4. The PDF is A4 landscape (1.414:1), which is also the proportion of a
     half-page of A4 portrait, so the figure embeds at any size without
     aspect-ratio distortion.
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
    import json

    import pandas as pd

    JOURNAL_DIR = "data/Collected_by_the_Journalist/"

    ZONES_OF_INTEREST = ("industrial", "tourism")
    ANCHOR_PLACE_INDUSTRIAL = "3753651731"  # High Seas Fishing Inc.
    ANCHOR_PLACE_TOURISM = "37828755"  # Dock & Roll Hall of Fame
    ANCHOR_PLAN_INDUSTRIAL = "deep_fishing_dock_Travel_High_Seas_Fishing_Inc"
    ANCHOR_PLAN_TOURISM = "seafood_festival_Travel_Dock_Roll_Hall_of_Fame"

    BOARD_SIZE = 6

    C = {
        "ind_dark": "#b45309",
        "ind_light": "#fde68a",
        "ind_agg": "#fef3c7",
        "tou_dark": "#0d9488",
        "tou_light": "#99f6e4",
        "tou_agg": "#ccfbf1",
        "neutral": "#cbd5e1",
        "text": "#0f172a",
        "muted": "#64748b",
        "track": "#e2e8f0",
        "tick": "#1e293b",
    }

    def read_csv(name: str) -> pd.DataFrame:
        df = pd.read_csv(JOURNAL_DIR + name, dtype=str, keep_default_na=False)
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        return df

    people_df = read_csv("people.csv")
    plans_df = read_csv("plans.csv")
    places_df = read_csv("places.csv")
    topics_df = read_csv("topics.csv")
    plan_topics_df = read_csv("plan_topics.csv")
    ppp_df = read_csv("plan_people_participations.csv")
    tl_df = read_csv("travel_links.csv")
    tp_df = read_csv("trip_people.csv")
    tpl_df = read_csv("trip_places.csv")

    place_name_map = dict(zip(places_df["place_id"], places_df["name"]))
    place_zone_map = dict(zip(places_df["place_id"], places_df["zone"]))

    named_by_zone: dict[str, list[str]] = {z: [] for z in ZONES_OF_INTEREST}
    unnamed_by_zone: dict[str, list[str]] = {z: [] for z in ZONES_OF_INTEREST}
    for pid, zone in place_zone_map.items():
        if zone not in ZONES_OF_INTEREST:
            continue
        if place_name_map.get(pid):
            named_by_zone[zone].append(pid)
        else:
            unnamed_by_zone[zone].append(pid)

    PEOPLE_IDS = set(people_df["people_id"])

    visits_long = tp_df.merge(tpl_df, on="trip_id")[["place_id", "people_id"]]
    visitors_by_place: dict[str, set[str]] = {
        pid: set(g["people_id"]) & PEOPLE_IDS
        for pid, g in visits_long.groupby("place_id")
    }

    plan_to_planners: dict[str, set[str]] = {
        plan_id: set(g["people_id"]) & PEOPLE_IDS
        for plan_id, g in ppp_df.groupby("plan_id")
    }

    planners_by_place: dict[str, set[str]] = {}
    for _, r in tl_df.iterrows():
        pid = r["place_id"]
        if pid not in place_zone_map:
            continue
        planners_by_place.setdefault(pid, set()).update(
            plan_to_planners.get(r["plan_id"], set())
        )

    def place_color(zone: str, is_anchor: bool, is_agg: bool) -> str:
        if is_anchor:
            return C["ind_dark"] if zone == "industrial" else C["tou_dark"]
        if is_agg:
            return C["ind_agg"] if zone == "industrial" else C["tou_agg"]
        return C["ind_light"] if zone == "industrial" else C["tou_light"]

    place_rows: list[dict] = []
    for zone in ZONES_OF_INTEREST:
        anchor_pid = (
            ANCHOR_PLACE_INDUSTRIAL if zone == "industrial" else ANCHOR_PLACE_TOURISM
        )
        named_rows: list[dict] = []
        for pid in named_by_zone[zone]:
            planned = planners_by_place.get(pid, set())
            visited = visitors_by_place.get(pid, set())
            named_rows.append(
                {
                    "id": pid,
                    "label": place_name_map[pid],
                    "zone": zone,
                    "is_anchor": pid == anchor_pid,
                    "is_agg": False,
                    "planned_set": planned,
                    "visited_set": visited,
                    "planned_count": len(planned),
                    "visited_count": len(visited),
                }
            )
        named_rows.sort(
            key=lambda r: (not r["is_anchor"], -r["visited_count"], r["label"].lower())
        )
        for nr in named_rows:
            nr["color"] = place_color(zone, nr["is_anchor"], False)
        place_rows.extend(named_rows)

        if unnamed_by_zone[zone]:
            agg_planned: set[str] = set()
            agg_visited: set[str] = set()
            for pid in unnamed_by_zone[zone]:
                agg_planned |= planners_by_place.get(pid, set())
                agg_visited |= visitors_by_place.get(pid, set())
            place_rows.append(
                {
                    "id": f"__agg_{zone}",
                    "agg_ids": list(unnamed_by_zone[zone]),
                    "label": f"({len(unnamed_by_zone[zone])} unnamed {zone} sites)",
                    "zone": zone,
                    "is_anchor": False,
                    "is_agg": True,
                    "planned_set": agg_planned,
                    "visited_set": agg_visited,
                    "planned_count": len(agg_planned),
                    "visited_count": len(agg_visited),
                    "color": place_color(zone, False, True),
                }
            )

    industrial_tourism_places = {
        pid for pid, z in place_zone_map.items() if z in ZONES_OF_INTEREST
    }

    travel_plan_ids = set(
        plans_df[plans_df["plan_type"].str.lower() == "travel"]["plan_id"]
    )

    plan_dest: dict[str, str] = {}
    for _, r in tl_df.iterrows():
        if (
            r["plan_id"] in travel_plan_ids
            and r["place_id"] in industrial_tourism_places
            and place_name_map.get(r["place_id"], "") != ""
        ):
            plan_dest[r["plan_id"]] = r["place_id"]

    kept_plan_ids = set(plan_dest.keys())

    plan_to_topic = dict(zip(plan_topics_df["plan_id"], plan_topics_df["topic_id"]))
    topic_long = dict(zip(topics_df["topic_id"], topics_df["long_topic"]))
    plan_long_title = dict(zip(plans_df["plan_id"], plans_df["long_title"]))

    plan_rows: list[dict] = []
    for pid in kept_plan_ids:
        dest = plan_dest[pid]
        zone = place_zone_map[dest]
        is_anchor = pid in (ANCHOR_PLAN_INDUSTRIAL, ANCHOR_PLAN_TOURISM)
        if is_anchor:
            color = C["ind_dark"] if zone == "industrial" else C["tou_dark"]
        else:
            color = C["ind_light"] if zone == "industrial" else C["tou_light"]
        plan_rows.append(
            {
                "id": pid,
                "label": plan_long_title.get(pid, pid),
                "topic_id": plan_to_topic.get(pid, ""),
                "zone": zone,
                "is_anchor": is_anchor,
                "dest_place_id": dest,
                "color": color,
            }
        )

    # Tag each topic with the dominant zone of its kept plans (industrial wins ties).
    topic_zone: dict[str, str] = {}
    for p in plan_rows:
        t = p["topic_id"]
        if t not in topic_zone:
            topic_zone[t] = p["zone"]
        elif topic_zone[t] != p["zone"] and p["zone"] == "industrial":
            topic_zone[t] = "industrial"

    anchor_topic_industrial = plan_to_topic.get(ANCHOR_PLAN_INDUSTRIAL, "")
    anchor_topic_tourism = plan_to_topic.get(ANCHOR_PLAN_TOURISM, "")

    def topic_sort_key(tid: str) -> tuple:
        zone = topic_zone.get(tid, "industrial")
        is_anchor_topic = tid in (anchor_topic_industrial, anchor_topic_tourism)
        zone_order = 0 if zone == "industrial" else 1
        return (zone_order, 0 if is_anchor_topic else 1, topic_long.get(tid, tid).lower())

    kept_topics = sorted({p["topic_id"] for p in plan_rows}, key=topic_sort_key)
    topic_order_idx = {t: i for i, t in enumerate(kept_topics)}
    place_order_idx = {r["id"]: i for i, r in enumerate(place_rows)}

    plan_rows.sort(
        key=lambda p: (
            topic_order_idx[p["topic_id"]],
            not p["is_anchor"],
            place_order_idx.get(p["dest_place_id"], 999),
        )
    )

    ROLE_ORDER = ["Committee Chair", "Vice Chair", "Treasurer", "Member"]
    role_idx = {r: i for i, r in enumerate(ROLE_ORDER)}

    people_rows: list[dict] = []
    for _, r in people_df.iterrows():
        people_rows.append(
            {"id": r["people_id"], "label": r["name"], "role": r["role"]}
        )
    people_rows.sort(key=lambda p: (role_idx.get(p["role"], 99), p["label"].lower()))

    def remap_place(pid: str) -> str | None:
        if pid not in place_zone_map:
            return None
        zone = place_zone_map[pid]
        if zone not in ZONES_OF_INTEREST:
            return None
        if place_name_map.get(pid, "") != "":
            return pid
        return f"__agg_{zone}"

    edges: list[dict] = []

    plan_id_to_row = {p["id"]: p for p in plan_rows}

    for p in plan_rows:
        edges.append(
            {
                "a_table": "plans",
                "a_id": p["id"],
                "b_table": "places",
                "b_id": p["dest_place_id"],
                "highlight": p["is_anchor"],
                "color": p["color"] if p["is_anchor"] else C["neutral"],
            }
        )

    kept_plan_set = set(plan_id_to_row.keys())
    for _, r in ppp_df.iterrows():
        if r["plan_id"] not in kept_plan_set:
            continue
        if r["people_id"] not in PEOPLE_IDS:
            continue
        plan = plan_id_to_row[r["plan_id"]]
        is_h = plan["is_anchor"]
        edges.append(
            {
                "a_table": "plans",
                "a_id": r["plan_id"],
                "b_table": "people",
                "b_id": r["people_id"],
                "highlight": is_h,
                "color": plan["color"] if is_h else C["neutral"],
            }
        )

    place_row_by_id = {r["id"]: r for r in place_rows}
    pp_seen: set[tuple[str, str]] = set()
    for _, r in visits_long.iterrows():
        if r["people_id"] not in PEOPLE_IDS:
            continue
        target = remap_place(r["place_id"])
        if target is None:
            continue
        key = (r["people_id"], target)
        if key in pp_seen:
            continue
        pp_seen.add(key)
        target_row = place_row_by_id.get(target)
        is_h = bool(target_row and target_row["is_anchor"])
        color = target_row["color"] if (is_h and target_row) else C["neutral"]
        edges.append(
            {
                "a_table": "people",
                "a_id": r["people_id"],
                "b_table": "places",
                "b_id": target,
                "highlight": is_h,
                "color": color,
            }
        )

    tables_data = {
        "people": [
            {"id": p["id"], "label": p["label"], "group": p["role"]}
            for p in people_rows
        ],
        "plans": [
            {
                "id": p["id"],
                "label": p["label"],
                "group": topic_long.get(p["topic_id"], p["topic_id"]),
                "color": p["color"],
                "is_anchor": p["is_anchor"],
                "zone": p["zone"],
            }
            for p in plan_rows
        ],
        "places": [
            {
                "id": p["id"],
                "label": p["label"],
                "group": p["zone"].title(),
                "color": p["color"],
                "is_anchor": p["is_anchor"],
                "is_agg": p["is_agg"],
                "zone": p["zone"],
                "planned": p["planned_count"],
                "visited": p["visited_count"],
            }
            for p in place_rows
        ],
    }

    payload = json.dumps(
        {
            "tables": tables_data,
            "edges": edges,
            "colors": C,
            "board_size": BOARD_SIZE,
        },
        ensure_ascii=False,
    )

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  @page { size: A4 landscape; margin: 0; }
  @media print {
    html, body { background: #ffffff; }
    .page { box-shadow: none; border: none; }
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    color: #0f172a; background: #f1f5f9;
  }
  body { overflow: hidden; }
  .page {
    width: 1122px; height: 794px;
    margin: 0 auto;
    background: #ffffff;
    position: relative;
    padding: 24px 28px 16px 28px;
    display: flex; flex-direction: column;
    gap: 10px;
  }
  .title {
    font-size: 19px; font-weight: 700; line-height: 1.30;
    color: #0f172a;
    letter-spacing: -0.01em;
  }
  .subtitle {
    font-size: 12.5px; font-weight: 500; line-height: 1.45;
    color: #334155;
  }
  .legend {
    font-size: 10.5px; color: #475569;
    display: flex; gap: 14px; align-items: center;
    padding-top: 2px;
  }
  .legend .swatch {
    display: inline-flex; align-items: center; gap: 5px;
  }
  .legend .bar-sw {
    display: inline-block; width: 22px; height: 9px;
    background: #94a3b8; border-radius: 1px;
  }
  .legend .tick-sw {
    display: inline-block; width: 2px; height: 14px; background: #1e293b;
  }
  .legend .track-sw {
    display: inline-block; width: 22px; height: 9px;
    background: #e2e8f0; border-radius: 1px;
  }
  .header-block { display: flex; flex-direction: column; gap: 6px; }
  .columns {
    flex: 1; min-height: 0;
    display: grid;
    grid-template-columns: 312px 158px 1fr;
    column-gap: 28px;
    position: relative;
    align-items: stretch;
  }
  .col { display: flex; flex-direction: column; min-height: 0; position: relative; z-index: 1; }
  .col-header {
    font-size: 9.5px; font-weight: 700; letter-spacing: 0.10em;
    text-transform: uppercase; color: #64748b;
    padding: 0 0 4px 0;
    border-bottom: 1px solid #cbd5e1;
    margin-bottom: 8px;
  }
  .col-body {
    flex: 1; min-height: 0;
    display: flex; flex-direction: column;
    justify-content: flex-start;
    gap: 0;
    overflow: hidden;
  }
  .col-people .col-body { justify-content: center; }
  .col-body-inner { display: flex; flex-direction: column; }
  .col-people .group + .group { margin-top: 14px; }
  .group { display: flex; flex-direction: column; }
  .group + .group { margin-top: 10px; }
  .group-header {
    font-size: 9px; font-weight: 700; color: #64748b;
    letter-spacing: 0.08em; text-transform: uppercase;
    padding: 1px 2px; border-bottom: 1px dotted #cbd5e1;
    margin-bottom: 3px;
  }
  .row {
    font-size: 11px; line-height: 14px;
    padding: 2px 4px;
    border-radius: 3px;
    color: #1e293b;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .col-plans .row {
    font-size: 10.5px; line-height: 13px;
    white-space: normal;
    padding: 2px 4px 3px 4px;
  }
  .row.anchor { font-weight: 700; }
  .place-row {
    display: grid;
    grid-template-columns: 190px 1fr;
    column-gap: 10px;
    align-items: center;
    padding: 2px 4px;
    border-radius: 3px;
    font-size: 11px; line-height: 14px;
    min-height: 16px;
  }
  .place-row.anchor .place-label { font-weight: 700; }
  .place-row.agg .place-label { font-style: italic; color: #475569; }
  .place-label { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .place-bullet { display: block; }
  .anchor-bg-ind { background: rgba(180, 83, 9, 0.10); }
  .anchor-bg-tou { background: rgba(13, 148, 136, 0.10); }
  .overlay {
    position: absolute; inset: 0;
    pointer-events: none;
    z-index: 0;
  }
  .overlay path { fill: none; stroke-linecap: round; }
  .footer {
    font-size: 9px; color: #94a3b8;
    line-height: 1.4;
    padding-top: 2px;
  }
</style>
</head>
<body>
<div class="page" id="page">
  <div class="header-block">
    <div class="title" id="title-text"></div>
    <div class="subtitle" id="subtitle-text"></div>
    <div class="legend" id="legend"></div>
  </div>
  <div class="columns" id="columns">
    <svg class="overlay" id="overlay"></svg>
    <div class="col col-plans">
      <div class="col-header">Plans (travel to industrial &amp; tourism places)</div>
      <div class="col-body"><div class="col-body-inner" id="col-plans"></div></div>
    </div>
    <div class="col col-people">
      <div class="col-header">Board members</div>
      <div class="col-body"><div class="col-body-inner" id="col-people"></div></div>
    </div>
    <div class="col col-places">
      <div class="col-header">Places &middot; planned vs visited</div>
      <div class="col-body"><div class="col-body-inner" id="col-places"></div></div>
    </div>
  </div>
  <div class="footer">
    Industrial &amp; Tourism zones only &middot; Unnamed sites aggregated per zone &middot;
    Counts are unique board members (of 6) &middot; Source: journalist field notes (data/Collected_by_the_Journalist).
  </div>
</div>
<script>
(() => {
  const DATA = __PAYLOAD__;
  const SVG_NS = "http://www.w3.org/2000/svg";
  const C = DATA.colors;
  const N = DATA.board_size;

  const ORDER = { plans: 0, people: 1, places: 2 };

  function keyOf(t, i) { return t + "\u0000" + i; }
  const rowElems = new Map();

  document.getElementById("title-text").innerHTML = (
    "<span style='color:" + C.ind_dark + "'>Fishing locations</span>" +
    " are often planned but unvisited. In contrast, some " +
    "<span style='color:" + C.tou_dark + "'>tourism locations</span>" +
    " are often visited outside of plans."
  );
  document.getElementById("subtitle-text").innerHTML = (
    "<span style='color:" + C.ind_dark + ";font-weight:700'>High Seas Fishing</span>" +
    " and " +
    "<span style='color:" + C.tou_dark + ";font-weight:700'>Dock &amp; Roll Hall of Fame</span>" +
    " were each planned to be visited by 1 board member. In reality, the former remained " +
    "unvisited, while the latter was visited by all 6."
  );
  document.getElementById("legend").innerHTML = (
    "<span class='swatch'><span class='bar-sw'></span>visited (bar)</span>" +
    "<span class='swatch'><span class='tick-sw'></span>planned (tick)</span>" +
    "<span class='swatch'><span class='track-sw'></span>remaining (track to 6)</span>" +
    "<span style='color:#94a3b8'>scale 0&ndash;" + N + " board members</span>"
  );

  function renderTable(tableKey, containerId, rowRenderer) {
    const container = document.getElementById(containerId);
    container.innerHTML = "";
    const entries = DATA.tables[tableKey];
    let currentGroup = null;
    let groupEl = null;
    for (const e of entries) {
      if (e.group !== currentGroup) {
        currentGroup = e.group;
        groupEl = document.createElement("div");
        groupEl.className = "group";
        const h = document.createElement("div");
        h.className = "group-header";
        h.textContent = e.group;
        groupEl.appendChild(h);
        container.appendChild(groupEl);
      }
      const rowEl = rowRenderer(e);
      rowEl.dataset.table = tableKey;
      rowEl.dataset.id = e.id;
      groupEl.appendChild(rowEl);
      rowElems.set(keyOf(tableKey, e.id), rowEl);
    }
  }

  renderTable("people", "col-people", (e) => {
    const r = document.createElement("div");
    r.className = "row";
    r.textContent = e.label;
    return r;
  });

  renderTable("plans", "col-plans", (e) => {
    const r = document.createElement("div");
    r.className = "row" + (e.is_anchor ? " anchor" : "");
    if (e.is_anchor) {
      r.classList.add(e.zone === "industrial" ? "anchor-bg-ind" : "anchor-bg-tou");
      r.style.color = e.color;
    }
    r.textContent = e.label;
    return r;
  });

  function bulletSVG(entry, innerW) {
    const xPlanned = (entry.planned / N) * innerW;
    const xVisited = (entry.visited / N) * innerW;
    const svgH = 18;
    const cy = svgH / 2;
    const barH = 10;
    const yBar = cy - barH / 2;
    const tickHalf = barH / 2 + 3;
    const fillColor = entry.color;
    const tickColor = C.tick;
    const labelText = entry.visited + " \u00b7 " + entry.planned;
    return (
      "<svg width='" + (innerW + 28) + "' height='" + svgH + "' xmlns='http://www.w3.org/2000/svg' style='overflow:visible; display:block'>" +
        "<rect x='0' y='" + yBar + "' width='" + innerW + "' height='" + barH + "' fill='" + C.track + "' rx='1.5'/>" +
        (xVisited > 0
          ? "<rect x='0' y='" + yBar + "' width='" + xVisited + "' height='" + barH + "' fill='" + fillColor + "' rx='1.5'/>"
          : "") +
        (entry.planned > 0
          ? "<line x1='" + xPlanned + "' x2='" + xPlanned + "' y1='" + (cy - tickHalf) + "' y2='" + (cy + tickHalf) + "' stroke='" + tickColor + "' stroke-width='2'/>"
          : "") +
        "<text x='" + (innerW + 5) + "' y='" + (cy + 3.4) + "' font-size='9.5' fill='" + C.muted + "' font-family='inherit'>" +
          labelText +
        "</text>" +
      "</svg>"
    );
  }

  renderTable("places", "col-places", (e) => {
    const r = document.createElement("div");
    r.className = "place-row" + (e.is_anchor ? " anchor" : "") + (e.is_agg ? " agg" : "");
    if (e.is_anchor) {
      r.classList.add(e.zone === "industrial" ? "anchor-bg-ind" : "anchor-bg-tou");
    }
    const labelEl = document.createElement("div");
    labelEl.className = "place-label";
    labelEl.textContent = e.label;
    if (e.is_anchor) labelEl.style.color = e.color;
    const bulletEl = document.createElement("div");
    bulletEl.className = "place-bullet";
    r.appendChild(labelEl);
    r.appendChild(bulletEl);
    return r;
  });

  function renderBullets() {
    for (const e of DATA.tables.places) {
      const row = rowElems.get(keyOf("places", e.id));
      if (!row) continue;
      const bulletEl = row.querySelector(".place-bullet");
      if (!bulletEl) continue;
      const w = bulletEl.clientWidth || bulletEl.parentElement.clientWidth - 200;
      const innerW = Math.max(140, w - 32);
      bulletEl.innerHTML = bulletSVG(e, innerW);
    }
  }

  const overlay = document.getElementById("overlay");
  const columnsEl = document.getElementById("columns");

  function drawCurves() {
    const cRect = columnsEl.getBoundingClientRect();
    overlay.setAttribute("width", cRect.width);
    overlay.setAttribute("height", cRect.height);
    overlay.setAttribute("viewBox", "0 0 " + cRect.width + " " + cRect.height);
    while (overlay.firstChild) overlay.removeChild(overlay.firstChild);

    const colRects = {};
    for (const t of ["people", "plans", "places"]) {
      const colEl = document.querySelector(".col-" + t);
      const cr = colEl.getBoundingClientRect();
      colRects[t] = {
        left: cr.left - cRect.left,
        right: cr.right - cRect.left,
      };
    }

    const sorted = DATA.edges.slice().sort((a, b) => (a.highlight ? 1 : 0) - (b.highlight ? 1 : 0));
    for (const ed of sorted) {
      const ea = rowElems.get(keyOf(ed.a_table, ed.a_id));
      const eb = rowElems.get(keyOf(ed.b_table, ed.b_id));
      if (!ea || !eb) continue;
      const ra = ea.getBoundingClientRect();
      const rb = eb.getBoundingClientRect();

      let leftT = ed.a_table, rightT = ed.b_table;
      let leftRow = ra, rightRow = rb;
      if (ORDER[ed.a_table] > ORDER[ed.b_table]) {
        leftT = ed.b_table; rightT = ed.a_table;
        leftRow = rb; rightRow = ra;
      }

      const x1 = colRects[leftT].right - 2;
      const x2 = colRects[rightT].left + 2;
      const y1 = (leftRow.top + leftRow.bottom) / 2 - cRect.top;
      const y2 = (rightRow.top + rightRow.bottom) / 2 - cRect.top;
      const dx = x2 - x1;
      const cp = dx * 0.42;

      const path = document.createElementNS(SVG_NS, "path");
      path.setAttribute(
        "d",
        "M " + x1 + "," + y1 +
        " C " + (x1 + cp) + "," + y1 +
        " " + (x2 - cp) + "," + y2 +
        " " + x2 + "," + y2
      );
      path.setAttribute("stroke", ed.color);
      path.setAttribute("fill", "none");
      path.setAttribute("stroke-linecap", "round");
      path.setAttribute("stroke-width", ed.highlight ? "1.8" : "1");
      path.setAttribute("stroke-opacity", ed.highlight ? "0.92" : "0.22");
      overlay.appendChild(path);
    }
  }

  function relayout() {
    renderBullets();
    drawCurves();
  }

  window.addEventListener("load", relayout);
  window.addEventListener("resize", relayout);
  if (window.ResizeObserver) new ResizeObserver(relayout).observe(columnsEl);
  requestAnimationFrame(() => {
    relayout();
    requestAnimationFrame(relayout);
    setTimeout(relayout, 120);
  });
})();
</script>
</body>
</html>"""

    widget_html = HTML_TEMPLATE.replace("__PAYLOAD__", payload)

    iframe_html = (
        '<div style="display:flex; justify-content:center; overflow:auto;">'
        '<iframe srcdoc="'
        + _html.escape(widget_html, quote=True)
        + '" style="display:block; width:1122px; height:794px; border:1px solid #cbd5e1; '
        'border-radius:8px; background:#ffffff; flex-shrink:0;"></iframe>'
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
