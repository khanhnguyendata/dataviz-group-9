"""Board vs journalist schema discrepancies — interactive Marimo notebook."""

import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    from textwrap import dedent

    mo.md(
        dedent(
            r"""
            # Table schema with dataset discrepancies

            Compare **board** data (`data/Collected_by_the_Government/`) with **journalist** data (`data/Collected_by_the_Journalist/`).

            The interactive widget below spans the full browser width with three narrow lists — **plans** (left, showing each plan's long title), **people** (middle), and **places** (right) — separated by wide line bands. Each list is subdivided by section headers:

            - **people** grouped by `role`
            - **plans** grouped first by `topic` (from `plan_topics.csv` joined with `topics.csv`), then nested by `plan_type` (normalized to Title Case so labels like `Report` / `report` and `Take Action` / `take action` merge into a single sub-group)
            - **places** grouped by `zone`

            All plans and places are rendered in full inside the widget — the widget does not scroll internally, so an edge's endpoint always lands exactly on its row. Scroll the notebook page to traverse the widget vertically. Click any row to isolate its links; click again (or any blank area) to clear.

            **Row fill** (one per entity) encodes where that entity is recorded:

            | Fill | Meaning |
            |------|---------|
            | Light | Present in **both** datasets |
            | Red   | Only in the **journalist** export (omitted by the board) |

            (The board dataset is a strict subset of the journalist's, so no row is ever board-only.)

            **Line color** encodes the dataset status of the relationship itself:

            | Line color | Meaning |
            |------------|---------|
            | Gray   | **Both** — every supporting record is shared |
            | Amber  | **Partial** — *people ↔ places* only: board admits the visit but records fewer trips than the journalist |
            | Red    | **Journalist only** — no board record at all |

            **Line style** encodes plan attribution (only varies on *people ↔ places* edges):

            | Line style | Meaning |
            |------------|---------|
            | Solid  | **Plan-attributable** — some plan lists both the person (via `plan_people_participations`) and the place (via `travel_links`) |
            | Dashed | **Off-plan** — no plan in either dataset covers this (person, place) pair |

            Edges between `people ↔ plans` and `plans ↔ places` are always solid (they come directly from `plan_people_participations.csv` and `travel_links.csv`, so plan attribution is trivial). The `people ↔ places` edges are derived from `trip_people.csv ⋈ trip_places.csv` on `trip_id`.
            """
        ).strip()
    )
    return


@app.cell
def _(mo):
    from collections import Counter
    from pathlib import Path
    import html as _html
    import json

    import pandas as pd

    ROOT = Path(__file__).resolve().parent
    BOARD = ROOT / "data" / "Collected_by_the_Government"
    JOURNAL = ROOT / "data" / "Collected_by_the_Journalist"

    TABLES = ["people", "plans", "places"]
    GROUP_COLS = {"people": "role", "plans": "plan_type", "places": "zone"}

    def read_csv(path: Path) -> pd.DataFrame:
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    def group_value(table: str, row: pd.Series) -> str:
        raw = str(row[GROUP_COLS[table]]).strip()
        if not raw:
            return "(unspecified)"
        if table == "plans":
            return raw.title()
        return raw

    # Build plan_id -> topic short name (using the journalist superset).
    _plan_topic_df = read_csv(JOURNAL / "plan_topics.csv").merge(
        read_csv(JOURNAL / "topics.csv"), on="topic_id", how="left"
    )
    PLAN_TOPIC = {
        str(pid).strip(): str(st).strip()
        for pid, st in zip(_plan_topic_df["plan_id"], _plan_topic_df["short_topic"])
    }

    def group_subgroup(table: str, row: pd.Series) -> tuple[str, str | None]:
        if table == "plans":
            plan_id = str(row["plan_id"]).strip()
            topic = PLAN_TOPIC.get(plan_id, "").strip() or "(no topic)"
            ptype = str(row["plan_type"]).strip().title() or "(unspecified)"
            return topic, ptype
        return group_value(table, row), None

    def classify_by_pk(
        pk: str, bdf: pd.DataFrame, jdf: pd.DataFrame
    ) -> tuple[set[str], set[str], set[str]]:
        b = set(bdf[pk].astype(str).str.strip())
        j = set(jdf[pk].astype(str).str.strip())
        return b & j, b - j, j - b

    def membership_of(k: str, both: set, bonly: set, jonly: set) -> str:
        if k in both:
            return "both"
        if k in bonly:
            return "board_only"
        if k in jonly:
            return "journal_only"
        return "both"

    def display_name(table: str, row: pd.Series) -> str:
        if table in ("people", "places"):
            return str(row["name"]).strip()
        if table == "plans":
            return str(row["long_title"]).strip()
        return "?"

    people_b = read_csv(BOARD / "people.csv")
    people_j = read_csv(JOURNAL / "people.csv")
    plans_b = read_csv(BOARD / "plans.csv")
    plans_j = read_csv(JOURNAL / "plans.csv")
    places_b = read_csv(BOARD / "places.csv")
    places_j = read_csv(JOURNAL / "places.csv")

    pb, p_only_b, p_only_j = classify_by_pk("people_id", people_b, people_j)
    plb, pl_only_b, pl_only_j = classify_by_pk("plan_id", plans_b, plans_j)
    plab, pla_only_b, pla_only_j = classify_by_pk("place_id", places_b, places_j)

    ROW_MEMBERSHIP_ORDER = {"both": 0, "board_only": 1, "journal_only": 2}
    EDGE_MEMBERSHIP_ORDER = {"both": 0, "partial": 1, "journal_only": 2}

    def build_table_entries(
        table: str,
        pk: str,
        both: set,
        bonly: set,
        jonly: set,
        dfb: pd.DataFrame,
        dfj: pd.DataFrame,
    ) -> list[dict]:
        universe = both | bonly | jonly
        entries: list[dict] = []
        for k in sorted(universe):
            sk = str(k).strip()
            rb = dfb[dfb[pk].astype(str).str.strip() == sk]
            rj = dfj[dfj[pk].astype(str).str.strip() == sk]
            row = rb.iloc[0] if len(rb) else rj.iloc[0]
            g, sg = group_subgroup(table, row)
            entries.append(
                {
                    "id": sk,
                    "label": display_name(table, row),
                    "membership": membership_of(sk, both, bonly, jonly),
                    "group": g,
                    "subgroup": sg,
                }
            )
        entries.sort(
            key=lambda e: (
                e["group"].lower(),
                (e["subgroup"] or "").lower(),
                ROW_MEMBERSHIP_ORDER[e["membership"]],
                e["label"].lower(),
            )
        )
        return entries

    tables_data = {
        "people": build_table_entries(
            "people", "people_id", pb, p_only_b, p_only_j, people_b, people_j
        ),
        "plans": build_table_entries(
            "plans", "plan_id", plb, pl_only_b, pl_only_j, plans_b, plans_j
        ),
        "places": build_table_entries(
            "places", "place_id", plab, pla_only_b, pla_only_j, places_b, places_j
        ),
    }

    valid_ids = {t: {e["id"] for e in entries} for t, entries in tables_data.items()}

    # --- per-junction edge extraction --------------------------------------

    def plan_people_edges(folder: Path) -> set[tuple[str, str]]:
        df = read_csv(folder / "plan_people_participations.csv")
        out: set[tuple[str, str]] = set()
        for _, r in df.iterrows():
            plan_id = str(r["plan_id"]).strip()
            people_id = str(r["people_id"]).strip()
            if plan_id in valid_ids["plans"] and people_id in valid_ids["people"]:
                out.add((plan_id, people_id))
        return out

    def travel_link_edges(folder: Path) -> set[tuple[str, str]]:
        df = read_csv(folder / "travel_links.csv")
        out: set[tuple[str, str]] = set()
        for _, r in df.iterrows():
            plan_id = str(r["plan_id"]).strip()
            place_id = str(r["place_id"]).strip()
            if plan_id in valid_ids["plans"] and place_id in valid_ids["places"]:
                out.add((plan_id, place_id))
        return out

    def people_place_trips(folder: Path) -> dict[tuple[str, str], set[str]]:
        """(people_id, place_id) -> set of trip_ids that witness the visit."""
        tp = read_csv(folder / "trip_people.csv")
        tpl = read_csv(folder / "trip_places.csv")
        merged = tp.merge(tpl, on="trip_id", how="inner", suffixes=("_p", "_q"))
        out: dict[tuple[str, str], set[str]] = {}
        for _, r in merged.iterrows():
            people_id = str(r["people_id"]).strip()
            place_id = str(r["place_id"]).strip()
            trip_id = str(r["trip_id"]).strip()
            if people_id in valid_ids["people"] and place_id in valid_ids["places"]:
                out.setdefault((people_id, place_id), set()).add(trip_id)
        return out

    def plan_attributable_set(folder: Path) -> set[tuple[str, str]]:
        """(people_id, place_id) pairs reachable via some plan P that rosters the
        person AND whose travel_links include the place."""
        ppp = read_csv(folder / "plan_people_participations.csv")
        tl = read_csv(folder / "travel_links.csv")
        joined = ppp[["plan_id", "people_id"]].merge(
            tl[["plan_id", "place_id"]], on="plan_id", how="inner"
        )
        out: set[tuple[str, str]] = set()
        for _, r in joined.iterrows():
            people_id = str(r["people_id"]).strip()
            place_id = str(r["place_id"]).strip()
            if people_id in valid_ids["people"] and place_id in valid_ids["places"]:
                out.add((people_id, place_id))
        return out

    pp_board = plan_people_edges(BOARD)
    pp_journ = plan_people_edges(JOURNAL)
    tl_board = travel_link_edges(BOARD)
    tl_journ = travel_link_edges(JOURNAL)
    trips_board = people_place_trips(BOARD)
    trips_journ = people_place_trips(JOURNAL)

    # Journalist is the superset, so use its plan-attribution set as the canonical reference.
    plan_attr = plan_attributable_set(JOURNAL)

    edges_data: list[dict] = []

    # plans <-> people: binary (both / journal_only), always plan-attributable
    for plan_id, people_id in sorted(pp_board | pp_journ):
        in_board = (plan_id, people_id) in pp_board
        in_journ = (plan_id, people_id) in pp_journ
        if in_board and in_journ:
            membership = "both"
        elif in_journ:
            membership = "journal_only"
        else:
            membership = "both"  # board-only is empty; defensive
        edges_data.append(
            {
                "a_table": "plans",
                "a_id": plan_id,
                "b_table": "people",
                "b_id": people_id,
                "membership": membership,
                "plan_attributable": True,
                "n_trips_both": None,
                "n_trips_journ_only": None,
            }
        )

    # plans <-> places: binary, always plan-attributable
    for plan_id, place_id in sorted(tl_board | tl_journ):
        in_board = (plan_id, place_id) in tl_board
        in_journ = (plan_id, place_id) in tl_journ
        if in_board and in_journ:
            membership = "both"
        elif in_journ:
            membership = "journal_only"
        else:
            membership = "both"
        edges_data.append(
            {
                "a_table": "plans",
                "a_id": plan_id,
                "b_table": "places",
                "b_id": place_id,
                "membership": membership,
                "plan_attributable": True,
                "n_trips_both": None,
                "n_trips_journ_only": None,
            }
        )

    # people <-> places: three-state membership based on supporting trip evidence
    all_pairs = set(trips_board) | set(trips_journ)
    for pair in sorted(all_pairs):
        b_trips = trips_board.get(pair, set())
        j_trips = trips_journ.get(pair, set())
        if not b_trips and not j_trips:
            continue
        if not b_trips:
            membership = "journal_only"
        elif b_trips == j_trips:
            membership = "both"
        else:
            membership = "partial"
        edges_data.append(
            {
                "a_table": "people",
                "a_id": pair[0],
                "b_table": "places",
                "b_id": pair[1],
                "membership": membership,
                "plan_attributable": pair in plan_attr,
                "n_trips_both": len(b_trips & j_trips),
                "n_trips_journ_only": len(j_trips - b_trips),
            }
        )

    entities_view = (
        pd.DataFrame(
            [
                {
                    "table": t,
                    "group": e["group"],
                    "subgroup": e.get("subgroup"),
                    "id": e["id"],
                    "label": e["label"],
                    "membership": e["membership"],
                }
                for t, entries in tables_data.items()
                for e in entries
            ]
        )
        .assign(
            _ord=lambda d: d["membership"].map(ROW_MEMBERSHIP_ORDER),
            _sub=lambda d: d["subgroup"].fillna(""),
        )
        .sort_values(["table", "group", "_sub", "_ord", "label"], kind="mergesort")
        .drop(columns=["_ord", "_sub"])
        .reset_index(drop=True)
    )

    pair_counts: dict[tuple[str, str], Counter] = {}
    for e in edges_data:
        pk_pair = tuple(sorted([e["a_table"], e["b_table"]]))
        pair_counts.setdefault(pk_pair, Counter())[e["membership"]] += 1
    edge_summary_df = pd.DataFrame(
        [
            {
                "table_a": a,
                "table_b": b,
                "both": c.get("both", 0),
                "partial": c.get("partial", 0),
                "journal_only": c.get("journal_only", 0),
                "total": c.get("both", 0)
                + c.get("partial", 0)
                + c.get("journal_only", 0),
            }
            for (a, b), c in sorted(pair_counts.items())
        ]
    )

    payload = json.dumps(
        {"tables": tables_data, "edges": edges_data}, ensure_ascii=False
    )

    ROW_H, HDR_H, COL_HDR_H, COL_PAD, SUB_HDR_H = 14, 18, 32, 24, 14
    column_heights = []
    for entries in tables_data.values():
        n_groups = len({e["group"] for e in entries})
        n_subs = len({(e["group"], e["subgroup"]) for e in entries if e.get("subgroup")})
        column_heights.append(
            len(entries) * ROW_H + n_groups * HDR_H + n_subs * SUB_HDR_H
        )
    iframe_height = max(column_heights) + COL_HDR_H + COL_PAD + 40

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f1f5f9; color: #0f172a; }
  .container {
    position: relative;
    padding: 12px;
    height: 100%;
    display: grid;
    grid-template-columns: 180px 1fr 140px 1fr 180px;
    grid-template-areas: "plans . people . places";
    column-gap: 0;
  }
  .column[data-table="plans"]  { grid-area: plans; }
  .column[data-table="people"] { grid-area: people; }
  .column[data-table="places"] { grid-area: places; }
  .column {
    display: flex; flex-direction: column;
    background: #ffffff; border: 1px solid #cbd5e1; border-radius: 8px;
    overflow: hidden; min-height: 0;
  }
  .column h3 {
    margin: 0; padding: 6px 10px; font-size: 12px; font-weight: 700;
    background: #e2e8f0; border-bottom: 1px solid #cbd5e1;
  }
  .list {
    flex: 1;
    overflow: hidden;
    padding: 4px 6px;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .list-inner {
    display: flex;
    flex-direction: column;
    margin: auto 0;
    width: 100%;
  }
  .group { display: flex; flex-direction: column; }
  .group-header {
    background: #f1f5f9;
    color: #334155;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 2px 4px;
    border-bottom: 1px solid #cbd5e1;
    margin-top: 4px;
  }
  .group-header:first-child { margin-top: 0; }
  .subgroup { display: flex; flex-direction: column; }
  .subgroup-header {
    font-size: 9px; font-weight: 600; color: #475569;
    text-transform: uppercase; letter-spacing: 0.05em;
    padding: 1px 4px 1px 10px;
    line-height: 12px;
  }
  .row {
    display: block; font-size: 11px; line-height: 13px;
    padding: 0 6px 1px;
    border-radius: 3px; cursor: pointer;
    border: none;
    user-select: none;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    transition: opacity .15s, box-shadow .15s, filter .15s;
  }
  .row + .row { margin-top: 0; }
  .row.m-both         { background: #f1f5f9; color: #0f172a; }
  .row.m-journal_only { background: #ef4444; color: #fff1f1; }
  .row.m-both + .row.m-both { box-shadow: inset 0 1px 0 #e2e8f0; }
  .row:hover { box-shadow: 0 0 0 2px #3b82f6; }
  .container.has-selection .row { opacity: 0.18; }
  .container.has-selection .row.linked   { opacity: 1.0; }
  .container.has-selection .row.selected { opacity: 1.0; box-shadow: 0 0 0 3px #1d4ed8; filter: none; }
  .overlay { position: absolute; top: 0; left: 0; pointer-events: none; }
  .overlay line {
    stroke-linecap: round;
    stroke-opacity: 0.16;
    stroke-width: 1;
    transition: stroke-opacity .15s, stroke-width .15s;
  }
  .overlay line.line-both         { stroke: #64748b; }
  .overlay line.line-partial      { stroke: #f59e0b; }
  .overlay line.line-journal_only { stroke: #b91c1c; }
  .overlay line.off-plan          { stroke-dasharray: 5 4; }
  .container.has-selection .overlay line         { stroke-opacity: 0.04; }
  .container.has-selection .overlay line.active  { stroke-opacity: 1.0; stroke-width: 2.5; }
  .overlay line.active { stroke-opacity: 0.95; stroke-width: 2; }
  .legend {
    position: absolute; right: 18px; bottom: 14px;
    font-size: 11px; color: #334155;
    background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 6px;
    border: 1px solid #cbd5e1; pointer-events: none; max-width: 360px;
    line-height: 1.5;
  }
  .legend .group-label { display: block; font-weight: 700; margin-top: 2px; color: #0f172a; }
  .legend .sw {
    display: inline-block; width: 10px; height: 10px;
    margin-right: 4px; vertical-align: middle; border: 1px solid #94a3b8;
  }
  .legend .sw.both        { background: #f8fafc; }
  .legend .sw.partial     { background: #f59e0b; }
  .legend .sw.journal     { background: #ef4444; }
  .legend .line-sw {
    display: inline-block; width: 22px; height: 0; margin-right: 4px;
    vertical-align: middle; border-top: 2px solid #334155;
  }
  .legend .line-sw.dashed { border-top-style: dashed; }
  .hint { margin-top: 6px; opacity: 0.8; }
</style>
</head>
<body>
<div class="container" id="container">
  <div class="column" data-table="people">
    <h3 id="h-people"></h3>
    <div class="list" id="list-people"></div>
  </div>
  <div class="column" data-table="plans">
    <h3 id="h-plans"></h3>
    <div class="list" id="list-plans"></div>
  </div>
  <div class="column" data-table="places">
    <h3 id="h-places"></h3>
    <div class="list" id="list-places"></div>
  </div>
  <svg class="overlay" id="overlay"></svg>
  <div class="legend">
    <span class="group-label">Rows &amp; line color (dataset)</span>
    <span><span class="sw both"></span>Both datasets</span>
    <span style="margin-left:8px"><span class="sw partial"></span>Partial (board undercounts trips)</span>
    <span style="margin-left:8px"><span class="sw journal"></span>Journalist only</span>
    <span class="group-label">Line style (plan attribution)</span>
    <span><span class="line-sw"></span>Plan-attributable</span>
    <span style="margin-left:8px"><span class="line-sw dashed"></span>Off-plan</span>
    <div class="hint">Click a row to isolate its links. Click again (or any blank area) to clear.</div>
  </div>
</div>
<script>
(() => {
  const DATA = __PAYLOAD__;
  const SVG_NS = "http://www.w3.org/2000/svg";
  const TABLES = ["people", "plans", "places"];
  const ORDER = { plans: 0, people: 1, places: 2 };
  const container = document.getElementById("container");
  const overlay = document.getElementById("overlay");
  const keyOf = (t, i) => t + "\u0000" + i;

  const rowElements = new Map();
  for (const t of TABLES) {
    const ents = (DATA.tables && DATA.tables[t]) || [];
    document.getElementById("h-" + t).textContent = t + " (" + ents.length + ")";
    const list = document.getElementById("list-" + t);
    const listInner = document.createElement("div");
    listInner.className = "list-inner";
    let currentGroup = null;
    let currentSub = null;
    let groupEl = null;
    let subEl = null;
    for (const e of ents) {
      if (e.group !== currentGroup) {
        currentGroup = e.group;
        currentSub = null;
        groupEl = document.createElement("div");
        groupEl.className = "group";
        const header = document.createElement("div");
        header.className = "group-header";
        header.textContent = e.group;
        groupEl.appendChild(header);
        listInner.appendChild(groupEl);
      }
      if (e.subgroup && e.subgroup !== currentSub) {
        currentSub = e.subgroup;
        subEl = document.createElement("div");
        subEl.className = "subgroup";
        const subHeader = document.createElement("div");
        subHeader.className = "subgroup-header";
        subHeader.textContent = e.subgroup;
        subEl.appendChild(subHeader);
        groupEl.appendChild(subEl);
      } else if (!e.subgroup) {
        subEl = null;
        currentSub = null;
      }
      const row = document.createElement("div");
      row.className = "row m-" + e.membership;
      row.dataset.table = t;
      row.dataset.id = e.id;
      row.textContent = e.label || e.id;
      row.title = (e.label ? e.label + " \u2014 " : "") + e.id + " (" + e.membership + ")";
      (subEl || groupEl).appendChild(row);
      rowElements.set(keyOf(t, e.id), row);
    }
    list.appendChild(listInner);
  }


  const adjacency = new Map();
  const edgeLines = [];
  const edges = DATA.edges || [];
  const lineFrag = document.createDocumentFragment();
  for (const e of edges) {
    const ka = keyOf(e.a_table, e.a_id);
    const kb = keyOf(e.b_table, e.b_id);
    if (!rowElements.has(ka) || !rowElements.has(kb)) continue;
    if (!adjacency.has(ka)) adjacency.set(ka, new Set());
    if (!adjacency.has(kb)) adjacency.set(kb, new Set());
    adjacency.get(ka).add(kb);
    adjacency.get(kb).add(ka);
    const line = document.createElementNS(SVG_NS, "line");
    const cls = ["line-" + e.membership];
    if (e.plan_attributable === false) cls.push("off-plan");
    line.setAttribute("class", cls.join(" "));
    if (e.n_trips_both != null || e.n_trips_journ_only != null) {
      const nb = e.n_trips_both || 0;
      const nj = e.n_trips_journ_only || 0;
      line.dataset.nTripsBoth = String(nb);
      line.dataset.nTripsJournOnly = String(nj);
    }
    lineFrag.appendChild(line);
    edgeLines.push({
      line, a: ka, b: kb,
      aTable: e.a_table, bTable: e.b_table,
    });
  }
  overlay.appendChild(lineFrag);

  function elementForKey(key) {
    return rowElements.get(key);
  }

  let raf = 0;
  function scheduleLayout() {
    if (raf) return;
    raf = requestAnimationFrame(() => { raf = 0; layoutLines(); });
  }

  function layoutLines() {
    const cRect = container.getBoundingClientRect();
    overlay.setAttribute("width", cRect.width);
    overlay.setAttribute("height", cRect.height);

    const colRects = {};
    const listRects = {};
    for (const t of TABLES) {
      const col = document.querySelector('[data-table="' + t + '"]');
      const list = document.getElementById("list-" + t);
      const cr = col.getBoundingClientRect();
      const lr = list.getBoundingClientRect();
      colRects[t]  = { left: cr.left - cRect.left, right: cr.right - cRect.left };
      listRects[t] = { top: lr.top - cRect.top, bottom: lr.bottom - cRect.top };
    }

    for (const item of edgeLines) {
      const ea = elementForKey(item.a);
      const eb = elementForKey(item.b);
      if (!ea || !eb) { item.line.style.display = "none"; continue; }
      const ra = ea.getBoundingClientRect();
      const rb = eb.getBoundingClientRect();
      let ya = (ra.top - cRect.top) + ra.height / 2;
      let yb = (rb.top - cRect.top) + rb.height / 2;
      const lra = listRects[item.aTable];
      const lrb = listRects[item.bTable];
      ya = Math.max(lra.top + 3, Math.min(lra.bottom - 3, ya));
      yb = Math.max(lrb.top + 3, Math.min(lrb.bottom - 3, yb));
      let leftT = item.aTable, rightT = item.bTable;
      let leftY = ya, rightY = yb;
      if (ORDER[item.aTable] > ORDER[item.bTable]) {
        leftT = item.bTable; rightT = item.aTable;
        leftY = yb; rightY = ya;
      }
      const x1 = colRects[leftT].right - 4;
      const x2 = colRects[rightT].left + 4;
      item.line.setAttribute("x1", x1);
      item.line.setAttribute("y1", leftY);
      item.line.setAttribute("x2", x2);
      item.line.setAttribute("y2", rightY);
      item.line.style.display = "";
    }
  }

  let selectedKey = null;
  function clearSelection() {
    selectedKey = null;
    container.classList.remove("has-selection");
    container.querySelectorAll(".row.selected, .row.linked").forEach((r) => {
      r.classList.remove("selected", "linked");
    });
    overlay.querySelectorAll("line.active").forEach((l) => l.classList.remove("active"));
  }
  function selectRow(key) {
    clearSelection();
    const row = elementForKey(key);
    if (!row) return;
    selectedKey = key;
    container.classList.add("has-selection");
    row.classList.add("selected");
    const nbrs = adjacency.get(key);
    if (nbrs) {
      nbrs.forEach((n) => {
        const r = elementForKey(n);
        if (r) r.classList.add("linked");
      });
    }
    for (const item of edgeLines) {
      if (item.a === key || item.b === key) item.line.classList.add("active");
    }
  }

  container.addEventListener("click", (ev) => {
    const row = ev.target.closest(".row");
    if (!row) { clearSelection(); return; }
    const key = keyOf(row.dataset.table, row.dataset.id);
    if (key === selectedKey) clearSelection();
    else selectRow(key);
  });

  window.addEventListener("resize", scheduleLayout);
  if (window.ResizeObserver) {
    new ResizeObserver(scheduleLayout).observe(container);
  }
  requestAnimationFrame(() => {
    layoutLines();
    setTimeout(layoutLines, 60);
    setTimeout(layoutLines, 300);
  });
})();
</script>
</body>
</html>"""

    widget_html = HTML_TEMPLATE.replace("__PAYLOAD__", payload)
    iframe_html = (
        '<style>'
        '  .msd-fullwidth-wrap {'
        '    display: block;'
        '    box-sizing: border-box;'
        '    position: relative;'
        '    width: 100vw;'
        '    min-width: 100vw;'
        '    max-width: 100vw;'
        '    left: 50%;'
        '    right: 50%;'
        '    margin-left: -50vw;'
        '    margin-right: -50vw;'
        '    flex-shrink: 0;'
        '  }'
        '  /* Strip content-width and horizontal padding from marimo ancestors '
        '     that contain this widget (uses :has(), modern browsers). */'
        '  :has(> .msd-fullwidth-wrap),'
        '  :has(> * > .msd-fullwidth-wrap),'
        '  :has(> * > * > .msd-fullwidth-wrap),'
        '  :has(> * > * > * > .msd-fullwidth-wrap) {'
        '    max-width: none !important;'
        '  }'
        '</style>'
        '<div class="msd-fullwidth-wrap">'
        '<iframe srcdoc="'
        + _html.escape(widget_html, quote=True)
        + f'" style="display:block; width:100%; height:{iframe_height}px; border:1px solid #cbd5e1; border-radius:8px; background:#f1f5f9;"></iframe>'
        '</div>'
    )
    widget = mo.Html(iframe_html)
    return edge_summary_df, entities_view, widget


@app.cell
def _(widget):
    widget
    return


@app.cell
def _(entities_view, mo):
    mo.vstack(
        [
            mo.md(
                "## Entity overview — **people**, **plans**, **places** (one row per id; membership vs board/journalist)"
            ),
            mo.ui.dataframe(entities_view, page_size=25),
        ]
    )
    return


@app.cell
def _(edge_summary_df, mo):
    mo.vstack(
        [
            mo.md(
                "## Relationship counts per table pair (3-state dataset membership; **partial** only applies to **people↔places** and means the board records the pair with fewer supporting trips than the journalist)"
            ),
            mo.ui.dataframe(edge_summary_df, page_size=20),
        ]
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
