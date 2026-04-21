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
            - **plans** grouped by `topic` (from `plan_topics.csv` joined with `topics.csv`); each plan row is prefixed with an emoji indicating its `plan_type` (🗣️ Discussion, 💬 Feedback, 📄 Report, ✊ Take Action, 🚗 Travel, 📊 Presentation, 📝 Proposal)
            - **places** grouped by `zone`

            All plans and places are rendered in full inside the widget — the widget does not scroll internally, so an edge's endpoint always lands exactly on its row. Scroll the notebook page to traverse the widget vertically. Click any row to isolate its links (clicking a person also highlights the places reachable via that person's plans); click again or any blank area to clear.

            Use the **toolbar** at the top of the chart to switch between three views: **All data** (default, every row and link), **Common only** (rows and links present in both datasets — the board's acknowledged record), or **Suppressed by board** (rows and links the board omitted or undercounted, including fully-recorded people whose individual trips or participations the board left out).

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

            **Line style** encodes plan attribution:

            | Line style | Meaning |
            |------------|---------|
            | Dotted | **Plan-attributable** — for `people ↔ places` edges: some plan lists both the person and the place; for `plans ↔ people` edges: the plan is a Travel plan; all `plans ↔ places` edges are always dotted |
            | Solid  | **Off-plan** — no plan in either dataset covers this (person, place) pair |

            The `people ↔ places` edges are derived from `trip_people.csv ⋈ trip_places.csv` on `trip_id`.
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

    # Build plan_id -> earliest meeting number so plans sort by when they appeared
    _mp_df = read_csv(JOURNAL / "meeting_plans.csv")
    _mp_df["_num"] = pd.to_numeric(
        _mp_df["meeting_id"].str.extract(r"(\d+)", expand=False), errors="coerce"
    ).fillna(999)
    PLAN_MEETING_NUM: dict[str, int] = (
        _mp_df.groupby(_mp_df["plan_id"].str.strip())["_num"].min().astype(int).to_dict()
    )

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

    PLAN_TYPE_EMOJI = {
        "Discussion": "\U0001f5e3\ufe0f",
        "Feedback": "\U0001f4ac",
        "Report": "\U0001f4c4",
        "Take Action": "\u270a",
        "Travel": "\U0001f697",
        "Presentation": "\U0001f4ca",
        "Proposal": "\U0001f4dd",
    }

    def display_name(table: str, row: pd.Series) -> str:
        if table in ("people", "places"):
            return str(row["name"]).strip()
        if table == "plans":
            ptype = str(row["plan_type"]).strip().title()
            emoji = PLAN_TYPE_EMOJI.get(ptype, "\u25cf")
            return f"{emoji} {str(row['long_title']).strip()}"
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
                PLAN_MEETING_NUM.get(e["id"], 999),
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

    # plan_id -> Title-Cased plan_type (used to decide dotted vs solid on plans<->people edges)
    PLAN_TYPE_MAP = {
        str(r["plan_id"]).strip(): str(r["plan_type"]).strip().title()
        for _, r in plans_j.iterrows()
    }

    edges_data: list[dict] = []

    # plans <-> people: dotted only when the plan is a Travel plan
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
                "plan_attributable": PLAN_TYPE_MAP.get(plan_id, "") == "Travel",
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

    ROW_H, HDR_H, COL_HDR_H, COL_PAD, GROUP_GAP = 14, 18, 32, 24, 30
    column_heights = []
    for t, entries in tables_data.items():
        n_groups = len({e["group"] for e in entries})
        row_h = ROW_H * 2 if t == "plans" else ROW_H  # plans rows may wrap to ~2 lines
        column_heights.append(
            len(entries) * row_h + n_groups * HDR_H + max(0, n_groups - 1) * GROUP_GAP
        )
    iframe_height = max(column_heights) + COL_HDR_H + COL_PAD + 40

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f1f5f9; color: #0f172a; }
  .middle-stack {
    grid-area: people;
    display: flex; flex-direction: column; align-items: stretch;
    min-height: 0;
  }
  .info-area {
    display: flex; flex-direction: column; align-items: center; gap: 8px;
    padding-bottom: 10px;
  }
  .toolbar { display: flex; justify-content: center; gap: 6px; }
  .mode-btn {
    font-size: 11px; padding: 4px 10px; border-radius: 4px;
    border: 1px solid #cbd5e1; background: #fff; color: #334155;
    cursor: pointer; user-select: none;
  }
  .mode-btn:hover { background: #f1f5f9; }
  .mode-btn.active { background: #1d4ed8; color: #fff; border-color: #1d4ed8; }
  .container {
    position: relative;
    padding: 12px;
    height: 100%;
    display: grid;
    grid-template-columns: 360px 1fr 140px 1fr 180px;
    grid-template-areas: "plans . people . places";
    column-gap: 0;
    align-items: center;
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
    padding: 4px 6px;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .list-inner {
    display: flex;
    flex-direction: column;
    margin: auto 0;
    width: 100%;
  }
  .group { display: flex; flex-direction: column; }
  .group + .group { margin-top: 20px; }
  .group-header {
    background: #f1f5f9;
    color: #334155;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 2px 4px;
    border-bottom: 1px solid #cbd5e1;
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
  .column[data-table="plans"] .row {
    white-space: normal;
    overflow: visible;
    text-overflow: unset;
    line-height: 15px;
    padding-bottom: 3px;
  }
  .row + .row { margin-top: 0; }
  .row.m-both         { background: #f1f5f9; color: #0f172a; }
  .row.m-journal_only { background: #ede9fe; color: #3b0764; }
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
  .overlay line.line-partial      { stroke: #0891b2; }
  .overlay line.line-journal_only { stroke: #7c3aed; }
  .overlay line.off-plan          { stroke-dasharray: none; }
  .overlay line.on-plan           { stroke-dasharray: 2 4; }
  .container.has-selection .overlay line         { stroke-opacity: 0.04; }
  .container.has-selection .overlay line.active  { stroke-opacity: 1.0; stroke-width: 2.5; }
  .overlay line.active { stroke-opacity: 0.95; stroke-width: 2; }
  .legend {
    font-size: 11px; color: #334155;
    background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 6px;
    border: 1px solid #cbd5e1; max-width: 540px;
    line-height: 1.5;
  }
  .legend .group-label { display: block; font-weight: 700; margin-top: 2px; color: #0f172a; }
  .legend .sw {
    display: inline-block; width: 10px; height: 10px;
    margin-right: 4px; vertical-align: middle; border: 1px solid #94a3b8;
  }
  .legend .sw.both        { background: #f8fafc; }
  .legend .sw.partial     { background: #0891b2; }
  .legend .sw.journal     { background: #7c3aed; }
  .legend .line-sw {
    display: inline-block; width: 22px; height: 0; margin-right: 4px;
    vertical-align: middle; border-top: 2px solid #334155;
  }
  .legend .line-sw.dotted { border-top-style: dotted; }
  .hint { margin-top: 6px; opacity: 0.8; }
  .row.mode-hidden { display: none; }
  .overlay line.mode-hidden { display: none; }
  .group.mode-empty { display: none; }
</style>
</head>
<body>
<div class="container" id="container">
  <div class="middle-stack">
    <div class="info-area">
      <div class="toolbar" role="radiogroup" aria-label="Dataset view">
        <button type="button" class="mode-btn active" data-mode="all">All data</button>
        <button type="button" class="mode-btn" data-mode="common">Common only</button>
        <button type="button" class="mode-btn" data-mode="suppressed">Suppressed by board</button>
      </div>
      <div class="legend">
        <span class="group-label">Rows &amp; line color (dataset)</span>
        <span><span class="sw both"></span>Both datasets</span>
        <span style="margin-left:8px"><span class="sw partial"></span>Partial (board undercounts trips)</span>
        <span style="margin-left:8px"><span class="sw journal"></span>Journalist only</span>
        <span class="group-label">Line style (plan attribution)</span>
        <span><span class="line-sw dotted"></span>Planned trips</span>
        <span style="margin-left:8px"><span class="line-sw"></span>Other links (incl. unplanned trips)</span>
        <div class="hint">Click any row to isolate its links; click a blank area to clear. Clicking a person also highlights the plan↔place links for their planned trips.</div>
      </div>
    </div>
    <div class="column" data-table="people">
      <h3 id="h-people"></h3>
      <div class="list" id="list-people"></div>
    </div>
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
    let groupEl = null;
    for (const e of ents) {
      if (e.group !== currentGroup) {
        currentGroup = e.group;
        groupEl = document.createElement("div");
        groupEl.className = "group";
        const header = document.createElement("div");
        header.className = "group-header";
        header.textContent = e.group;
        groupEl.appendChild(header);
        listInner.appendChild(groupEl);
      }
      const row = document.createElement("div");
      row.className = "row m-" + e.membership;
      row.dataset.table = t;
      row.dataset.id = e.id;
      row.textContent = e.label || e.id;
      row.title = (e.label ? e.label + " \u2014 " : "") + e.id + " (" + e.membership + ")";
      groupEl.appendChild(row);
      rowElements.set(keyOf(t, e.id), row);
    }
    list.appendChild(listInner);
  }


  const adjacency = new Map();
  const planPlaces = new Map();  // planKey -> Set of placeKeys
  const planPeople = new Map();  // planKey -> Set of personKeys
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
    // Track plan -> places and plan -> people for transitive highlighting
    if (e.a_table === "plans" && e.b_table === "places") {
      if (!planPlaces.has(ka)) planPlaces.set(ka, new Set());
      planPlaces.get(ka).add(kb);
    } else if (e.a_table === "plans" && e.b_table === "people") {
      if (!planPeople.has(ka)) planPeople.set(ka, new Set());
      planPeople.get(ka).add(kb);
    }
    const line = document.createElementNS(SVG_NS, "line");
    const cls = ["line-" + e.membership];
    if (e.plan_attributable === true) cls.push("on-plan");
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
      membership: e.membership,
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
      if (item.line.classList.contains("mode-hidden")) { item.line.style.display = "none"; continue; }
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
    const nbrs = adjacency.get(key) || new Set();
    nbrs.forEach((n) => {
      const r = elementForKey(n);
      if (r) r.classList.add("linked");
    });
    for (const item of edgeLines) {
      if (item.a === key || item.b === key) item.line.classList.add("active");
    }
    // Transitive: when a person is selected, also highlight plans' places
    if (row.dataset.table === "people") {
      const bridgedPlaceKeys = new Set();
      for (const planKey of nbrs) {
        if (!planKey.startsWith("plans\x00")) continue;
        const places = planPlaces.get(planKey);
        if (!places) continue;
        for (const placeKey of places) {
          if (placeKey === key) continue;
          if (!nbrs.has(placeKey)) {
            const pr = elementForKey(placeKey);
            if (pr) pr.classList.add("linked");
          }
          bridgedPlaceKeys.add(placeKey); // always track for plan<->place edge activation
        }
      }
      // Activate plan<->place edges that bridge to those newly linked places
      for (const item of edgeLines) {
        if (item.aTable === "plans" && item.bTable === "places") {
          const planKey = item.a;
          if (nbrs.has(planKey) && bridgedPlaceKeys.has(item.b)) {
            item.line.classList.add("active");
          }
        }
      }
    }
  }

  function applyMode() {
    const mode = container.dataset.mode || "all";
    // Step 1: mark lines visible/hidden by membership
    const visibleLineSet = new Set();
    for (const item of edgeLines) {
      let visible;
      if (mode === "all")         visible = true;
      else if (mode === "common") visible = item.membership === "both";
      else                        visible = item.membership !== "both";
      item.line.classList.toggle("mode-hidden", !visible);
      if (visible) visibleLineSet.add(item);
    }
    // Step 2: collect endpoint keys of visible lines
    const endpointKeys = new Set();
    for (const item of visibleLineSet) {
      endpointKeys.add(item.a);
      endpointKeys.add(item.b);
    }
    // Step 3: mark rows visible/hidden
    const visibleKeys = new Set();
    rowElements.forEach((el, key) => {
      const isBoth = el.classList.contains("m-both");
      let visible;
      if (mode === "all")         visible = true;
      else if (mode === "common") visible = isBoth;
      else                        visible = !isBoth || endpointKeys.has(key);
      el.classList.toggle("mode-hidden", !visible);
      if (visible) visibleKeys.add(key);
    });
    // Step 4: hide lines whose endpoint was hidden by row filter
    for (const item of visibleLineSet) {
      if (!visibleKeys.has(item.a) || !visibleKeys.has(item.b)) {
        item.line.classList.add("mode-hidden");
      }
    }
    // Step 5: collapse groups that have no visible rows
    document.querySelectorAll(".group").forEach(g => {
      g.classList.toggle("mode-empty", !g.querySelector(".row:not(.mode-hidden)"));
    });
    clearSelection();
    scheduleLayout();
  }

  container.dataset.mode = "all";
  document.querySelectorAll(".mode-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".mode-btn").forEach(b => b.classList.toggle("active", b === btn));
      container.dataset.mode = btn.dataset.mode;
      applyMode();
    });
  });

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
