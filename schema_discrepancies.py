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
        str(pid).strip(): str(lt).strip()
        for pid, lt in zip(_plan_topic_df["plan_id"], _plan_topic_df["long_topic"])
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
    iframe_height = max(column_heights) + COL_HDR_H + COL_PAD + 160

    HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; overflow: hidden; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f1f5f9; color: #0f172a; }
  .middle-stack {
    grid-area: people;
    display: flex; flex-direction: column; align-items: stretch;
    min-height: 0;
    width: 278px;
    justify-self: center;
  }
  .info-area {
    display: flex; flex-direction: column; align-items: stretch; gap: 8px;
    padding-bottom: 10px;
    width: 100%;
  }
  .toolbar { display: flex; gap: 6px; width: 100%; }
  .toolbar.hidden { display: none; }
  .mode-btn {
    flex: 1 1 0;
    font-size: 11px; padding: 4px 10px; border-radius: 4px;
    border: 1px solid #cbd5e1; background: #fff; color: #334155;
    cursor: pointer; user-select: none;
  }
  .mode-btn:hover { background: #f1f5f9; }
  .mode-btn.active,
  .mode-btn.active:hover,
  .mode-btn:active {
    color: #fff;
  }
  .mode-btn[data-mode="all"].active,
  .mode-btn[data-mode="all"].active:hover,
  .mode-btn[data-mode="all"]:active {
    background: #334155;
    border-color: #334155;
  }
  .mode-btn[data-mode="common"].active,
  .mode-btn[data-mode="common"].active:hover,
  .mode-btn[data-mode="common"]:active {
    background: #64748b;
    border-color: #64748b;
  }
  .mode-btn[data-mode="suppressed"].active,
  .mode-btn[data-mode="suppressed"].active:hover,
  .mode-btn[data-mode="suppressed"]:active {
    background: #7c3aed;
    border-color: #7c3aed;
  }
  .container {
    position: relative;
    padding: 12px;
    height: auto;
    display: grid;
    grid-template-columns: 360px 1fr 278px 1fr 180px;
    grid-template-areas: "plans . people . places";
    column-gap: 0px;
    align-items: start;
  }
  .column[data-table="plans"]  { grid-area: plans; }
  .column[data-table="people"] { grid-area: people; }
  .column[data-table="places"] { grid-area: places; }
  .column {
    display: flex; flex-direction: column;
    background: #ffffff; border: 1px solid #cbd5e1; border-radius: 8px;
    overflow: hidden; min-height: 0;
  }
  .column.collapsed {
    visibility: hidden;
    pointer-events: none;
    opacity: 0;
  }
  .column h3 {
    margin: 0; padding: 6px 10px; font-size: 12px; font-weight: 700;
    background: #334155; color: #ffffff; border-bottom: 1px solid #334155;
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
    background: #64748b;
    color: #ffffff;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 2px 4px;
    border-bottom: 1px solid #475569;
    cursor: pointer;
    user-select: none;
    transition: opacity .15s, box-shadow .15s, background .15s;
  }
  .group-header:hover { background: #475569; }
  .container.has-selection .group-header { opacity: 0.45; }
  .container.has-selection .group-header.related {
    opacity: 1.0;
  }
  .container.has-selection .group-header.selected {
    opacity: 1.0;
    background: #1d4ed8;
    box-shadow: 0 0 0 2px #1d4ed8;
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
  .row.m-both         { background: #f8fafc; color: #334155; }
  .row.m-journal_only { background: #f3e8ff; color: #5b21b6; }
  .row.m-both + .row.m-both { box-shadow: inset 0 1px 0 #e2e8f0; }
  .row:hover { box-shadow: 0 0 0 2px #3b82f6; }
  .container.has-selection .row { opacity: 0.18; }
  .container.has-selection .row.linked   { opacity: 1.0; }
  .container.has-selection .row.selected { opacity: 1.0; box-shadow: 0 0 0 3px #1d4ed8; filter: none; }
  .overlay { position: absolute; top: 0; left: 0; pointer-events: none; }
  .overlay path {
    fill: none;
    stroke-linecap: round;
    stroke-opacity: 0.28;
    stroke-width: 1.5;
    transition: stroke-opacity .15s, stroke-width .15s;
  }
  .overlay path.line-both         { stroke: #64748b; }
  .overlay path.line-partial      { stroke: #d97706; }
  .overlay path.line-journal_only { stroke: #7c3aed; }
  .overlay path.off-plan          { stroke-dasharray: none; }
  .overlay path.on-plan           { stroke-dasharray: none; }
  .container.has-selection .overlay path         { stroke-opacity: 0.06; }
  .container.has-selection .overlay path.active  { stroke-opacity: 1.0; stroke-width: 2; }
  .overlay path.active { stroke-opacity: 0.95; stroke-width: 2; }
  .legend {
    font-size: 11px; color: #334155;
    background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 6px;
    border: 1px solid #cbd5e1;
    line-height: 1.5;
  }
  .legend.hidden { display: none; }
  .legend .group-label { display: block; font-weight: 700; margin-top: 4px; color: #0f172a; }
  .legend .group-label:first-child { margin-top: 0; }
  .legend .legend-item { display: block; }
  .legend .sw {
    display: inline-block; width: 10px; height: 10px;
    margin-right: 4px; vertical-align: middle; border: 1px solid #94a3b8;
  }
  .legend .sw.both             { background: #f8fafc; }
  .legend .sw.journal          { background: #f3e8ff; border-color: #c4b5fd; }
  .legend .sw.edge-both        { background: #64748b; border-color: #64748b; }
  .legend .sw.edge-partial     { background: #d97706; border-color: #d97706; }
  .legend .sw.edge-journal     { background: #7c3aed; border-color: #7c3aed; }
  .row.mode-hidden { display: none; }
  .overlay path.mode-hidden { display: none; }
  .group.mode-empty { display: none; }
  .table-toggles {
    display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
  }
  .toggle-label {
    font-size: 11px; color: #64748b; font-weight: 600; flex-shrink: 0;
  }
  .tbl-btn {
    font-size: 11px; padding: 3px 10px; border-radius: 4px;
    border: 1px solid #cbd5e1; background: #fff; color: #334155;
    cursor: pointer; user-select: none; transition: background .12s, color .12s;
  }
  .tbl-btn:not([disabled]):hover { background: #f1f5f9; color: #334155; }
  .tbl-btn.active,
  .tbl-btn.active:hover,
  .tbl-btn:not([disabled]):active {
    background: #334155; color: #fff; border-color: #334155;
  }
  .tbl-btn[disabled] { cursor: default; opacity: 0.7; }
  .control-card {
    font-size: 11px; color: #334155;
    background: transparent; padding: 0; border-radius: 0;
    border: none;
    display: flex; flex-direction: column; gap: 6px;
    width: 100%;
  }
  .control-card .control-title {
    font-size: 10px; letter-spacing: 0.02em; text-transform: uppercase;
    color: #64748b; font-weight: 700;
  }
  .control-card.hidden { display: none; }
  .how-to-use {
    font-size: 11px; color: #334155;
    background: rgba(255,255,255,0.94); padding: 9px 12px; border-radius: 6px;
    border: 1px solid #cbd5e1; line-height: 1.55;
    width: 100%;
  }
  .instruction-collapsible {
    overflow: hidden;
    max-height: 420px;
    opacity: 1;
    transform: translateY(0);
    transition: max-height 420ms ease, opacity 320ms ease, transform 420ms ease, margin-top 420ms ease, padding-top 420ms ease, padding-bottom 420ms ease, border-color 320ms ease;
  }
  .instruction-collapsible.is-collapsed {
    max-height: 0;
    opacity: 0;
    transform: translateY(-6px);
    margin-top: 0;
    padding-top: 0;
    padding-bottom: 0;
    border-color: transparent;
    pointer-events: none;
  }
  .how-to-use > strong { display: block; margin: 0 0 6px 0; }
  .how-to-use p { margin: 0; }
  .how-to-use p + p { margin-top: 8px; }
  .how-to-use .control-card { margin-top: 6px; }
  .how-to-use summary {
    font-weight: 700; cursor: pointer; color: #0f172a; user-select: none;
    list-style: none; display: flex; align-items: center; gap: 4px;
  }
  .how-to-use summary::-webkit-details-marker { display: none; }
  .how-to-use summary::before { content: "▸"; font-size: 9px; color: #64748b; }
  .how-to-use[open] summary::before { content: "▾"; }
  .how-to-use summary:hover { color: #1d4ed8; }
  .usage-steps { margin: 6px 0 0 0; padding-left: 16px; }
  .usage-steps li { margin-bottom: 4px; }
  .usage-steps .control-card { margin-top: 6px; }
  .control-row { display: flex; gap: 6px; width: 100%; }
  .control-row .tbl-btn,
  .control-row .mode-btn { flex: 1 1 0; }
  .howto-legend { margin: 6px 0; display: flex; flex-direction: column; gap: 4px; }
  .howto-legend-item { display: flex; align-items: flex-start; gap: 6px; }
  .howto-legend-item .legend-label-common { color: #64748b; }
  .howto-legend-item .legend-label-partial { color: #d97706; }
  .howto-legend-item .legend-label-journal { color: #7c3aed; }
  .howto-swatch {
    display: inline-block; width: 10px; height: 10px; border-radius: 2px;
    border: 1px solid #94a3b8; flex: 0 0 10px;
    margin-top: 3px;
  }
  .howto-swatch.common { background: #64748b; border-color: #64748b; }
  .howto-swatch.partial { background: #d97706; border-color: #d97706; }
  .howto-swatch.journal { background: #7c3aed; border-color: #7c3aed; }
  .status-hint {
    font-size: 10px; color: #64748b; min-height: 14px; padding: 1px 2px;
    font-style: italic;
    display: none;
  }
  .hidden { display: none !important; }
</style>
</head>
<body>
<div class="container" id="container">
  <div class="middle-stack">
    <div class="info-area">
      <div class="how-to-use">
        <strong>1. How to read this chart</strong>
        <p>The board reports are missing a lot of data compared to yours — especially <strong>plans</strong> that <strong>people</strong> participated in, and <strong>places</strong> that they travelled to.</p>
        <p>This chart helps you spot missing links between <strong>plans</strong>&nbsp;↔ <strong>people</strong>&nbsp;↔ <strong>places</strong> in board data, highlighting potential biases that the board might try to hide from their reporting.</p>
      </div>
      <div class="how-to-use">
        <strong>2. Show related tables for each person</strong>
        <p>Below are the list of people in the board.</p>
        <p><strong>Click on Plans</strong> to display the plans associated to each person, and <strong>Places</strong> to display the places that they travelled to:</p>
        <div class="control-card">
          <div class="control-row" role="group" aria-label="Show columns">
            <button type="button" class="tbl-btn" data-tbl="plans">Plans</button>
            <button type="button" class="tbl-btn" data-tbl="places">Places</button>
          </div>
        </div>
      </div>
      <div class="how-to-use instruction-collapsible is-collapsed" id="instruction-3">
        <strong>3. Filter by dataset and read colors</strong>
        <p>The color of each plan or place reflects whether they appear in:</p>
        <div class="howto-legend">
          <div class="howto-legend-item"><span class="howto-swatch common"></span><span><strong class="legend-label-common">Common data:</strong> in both board data and your collected data</span></div>
          <div class="howto-legend-item"><span class="howto-swatch partial"></span><span><strong class="legend-label-partial">Partially missing trips:</strong> board data has fewer trips than your data</span></div>
          <div class="howto-legend-item"><span class="howto-swatch journal"></span><span><strong class="legend-label-journal">Journalist data only:</strong> only in your collected data</span></div>
        </div>
        <p>The same color logic also applies to <strong>links</strong> between <strong>plans</strong>&nbsp;↔ <strong>people</strong>&nbsp;↔ <strong>places</strong>.</p>
        <p><strong>Click on a dataset</strong> to filter data in that dataset:</p>
        <div class="control-card">
          <div class="toolbar" role="radiogroup" aria-label="Dataset view">
            <button type="button" class="mode-btn active" data-mode="all">All data</button>
            <button type="button" class="mode-btn" data-mode="common">Common data</button>
            <button type="button" class="mode-btn" data-mode="suppressed">Journalist data only</button>
          </div>
        </div>
      </div>
      <div class="how-to-use instruction-collapsible is-collapsed" id="instruction-4">
        <strong>4. Explore links to a given plan/person/place</strong>
        <p><strong>Click on a person, plan, or place</strong> to highlight linked entities.</p>
        <p><em>Tip:</em> <strong>Click on a plan topic, person role, or place zone</strong> to see the corresponding links across plans, people, and places under it.</p>
      </div>
      <div class="status-hint" id="status-hint"></div>
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
  const TABLE_LABELS = { people: "People", plans: "Plans", places: "Places" };
  for (const t of TABLES) {
    const ents = (DATA.tables && DATA.tables[t]) || [];
    document.getElementById("h-" + t).textContent = TABLE_LABELS[t] + " (" + ents.length + ")";
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
    const line = document.createElementNS(SVG_NS, "path");
    const cls = ["line-" + e.membership];
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
    raf = requestAnimationFrame(() => { raf = 0; layoutLines(); syncFrameHeight(); });
  }

  function syncFrameHeight() {
    if (!window.frameElement) return;
    const bodyH = document.body.scrollHeight;
    const docH = document.documentElement.scrollHeight;
    const nextH = Math.max(bodyH, docH, 400);
    window.frameElement.style.height = nextH + "px";
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
      const cp = (x2 - x1) * 0.42;
      const d = `M ${x1},${leftY} C ${x1 + cp},${leftY} ${x2 - cp},${rightY} ${x2},${rightY}`;
      item.line.setAttribute("d", d);
      item.line.style.display = "";
    }
  }

  let selectedKey = null;
  let selectedGroup = null;
  function clearSelection() {
    selectedKey = null;
    selectedGroup = null;
    container.classList.remove("has-selection");
    container.querySelectorAll(".row.selected, .row.linked, .group-header.selected, .group-header.related").forEach((r) => {
      r.classList.remove("selected", "linked", "related");
    });
    overlay.querySelectorAll("path.active").forEach((l) => l.classList.remove("active"));
    const hint = document.getElementById("status-hint");
    if (hint) hint.textContent = "";
  }
  function applySelection(selectedKeys) {
    container.classList.add("has-selection");
    for (const key of selectedKeys) {
      const r = elementForKey(key);
      if (r) r.classList.add("selected");
    }
    // Linked neighbors (outside the selected set)
    const linkedKeys = new Set();
    for (const key of selectedKeys) {
      const nbrs = adjacency.get(key) || new Set();
      nbrs.forEach((n) => {
        if (!selectedKeys.has(n)) linkedKeys.add(n);
      });
    }
    for (const key of linkedKeys) {
      const r = elementForKey(key);
      if (r) r.classList.add("linked");
    }
    // Active edges: any edge touching a selected key
    for (const item of edgeLines) {
      if (selectedKeys.has(item.a) || selectedKeys.has(item.b)) {
        item.line.classList.add("active");
      }
    }
    // Transitive: for each selected person, highlight places reachable via plans
    for (const key of selectedKeys) {
      const r = elementForKey(key);
      if (!r || r.dataset.table !== "people") continue;
      const nbrs = adjacency.get(key) || new Set();
      const bridgedPlaceKeys = new Set();
      for (const planKey of nbrs) {
        if (!planKey.startsWith("plans\x00")) continue;
        const places = planPlaces.get(planKey);
        if (!places) continue;
        for (const placeKey of places) {
          if (placeKey === key) continue;
          if (!selectedKeys.has(placeKey) && !nbrs.has(placeKey)) {
            const pr = elementForKey(placeKey);
            if (pr) pr.classList.add("linked");
          }
          bridgedPlaceKeys.add(placeKey);
        }
      }
      for (const item of edgeLines) {
        if (item.aTable === "plans" && item.bTable === "places") {
          if (nbrs.has(item.a) && bridgedPlaceKeys.has(item.b)) {
            item.line.classList.add("active");
          }
        }
      }
    }
    // Mark group headers that contain any linked row as "related"
    container.querySelectorAll(".row.linked").forEach((r) => {
      const g = r.closest(".group");
      if (!g) return;
      const h = g.querySelector(".group-header");
      if (h && !h.classList.contains("selected")) h.classList.add("related");
    });
  }
  function selectRow(key) {
    clearSelection();
    const row = elementForKey(key);
    if (!row) return;
    selectedKey = key;
    applySelection(new Set([key]));
  }
  function selectGroup(groupEl) {
    clearSelection();
    const keys = new Set();
    groupEl.querySelectorAll(".row:not(.mode-hidden)").forEach((r) => {
      keys.add(keyOf(r.dataset.table, r.dataset.id));
    });
    if (keys.size === 0) return;
    selectedGroup = groupEl;
    const header = groupEl.querySelector(".group-header");
    if (header) header.classList.add("selected");
    applySelection(keys);
  }

  const tableVisible = { people: true, plans: false, places: false };
  const plansCol = document.querySelector('.column[data-table="plans"]');
  const placesCol = document.querySelector('.column[data-table="places"]');
  const instruction3 = document.getElementById("instruction-3");
  const instruction4 = document.getElementById("instruction-4");
  let layoutResyncTimer = null;

  function resyncLayoutDuringInstructionAnimation() {
    if (layoutResyncTimer) {
      clearInterval(layoutResyncTimer);
      layoutResyncTimer = null;
    }
    // Recompute connector geometry while instructions are animating open/closed.
    // This keeps line anchors locked to rows during the unfurl transition.
    let ticks = 0;
    layoutResyncTimer = setInterval(() => {
      scheduleLayout();
      ticks += 1;
      if (ticks >= 12) {
        clearInterval(layoutResyncTimer);
        layoutResyncTimer = null;
      }
    }, 50);
  }

  function updateGridLayout() {
    if (plansCol) plansCol.classList.toggle("collapsed", !tableVisible.plans);
    if (placesCol) placesCol.classList.toggle("collapsed", !tableVisible.places);
    const hasRelatedTables = tableVisible.plans || tableVisible.places;
    if (instruction3) instruction3.classList.toggle("is-collapsed", !hasRelatedTables);
    if (instruction4) instruction4.classList.toggle("is-collapsed", !hasRelatedTables);
    resyncLayoutDuringInstructionAnimation();
  }

  function applyMode() {
    const mode = container.dataset.mode || "all";
    // Step 1: mark paths visible/hidden by membership and table visibility
    const visibleLineSet = new Set();
    for (const item of edgeLines) {
      let visible;
      if (mode === "all")         visible = true;
      else if (mode === "common") visible = item.membership === "both" || item.membership === "partial";
      else                        visible = item.membership === "journal_only";
      if (!tableVisible[item.aTable] || !tableVisible[item.bTable]) visible = false;
      item.line.classList.toggle("mode-hidden", !visible);
      if (visible) visibleLineSet.add(item);
    }
    // Step 2: collect endpoint keys of visible paths
    const endpointKeys = new Set();
    for (const item of visibleLineSet) {
      endpointKeys.add(item.a);
      endpointKeys.add(item.b);
    }
    // Step 3: mark rows visible/hidden
    const visibleKeys = new Set();
    rowElements.forEach((el, key) => {
      const isBoth = el.classList.contains("m-both");
      const t = el.dataset.table;
      let visible;
      if (!tableVisible[t])       visible = false;
      else if (mode === "all")    visible = true;
      else if (mode === "common") visible = isBoth;
      else                        visible = !isBoth || endpointKeys.has(key);
      el.classList.toggle("mode-hidden", !visible);
      if (visible) visibleKeys.add(key);
    });
    // Step 4: hide paths whose endpoint was hidden by row filter
    for (const item of visibleLineSet) {
      if (!visibleKeys.has(item.a) || !visibleKeys.has(item.b)) {
        item.line.classList.add("mode-hidden");
      }
    }
    // Step 5: collapse groups that have no visible rows
    document.querySelectorAll(".group").forEach(g => {
      g.classList.toggle("mode-empty", !g.querySelector(".row:not(.mode-hidden)"));
    });
    // Step 6: preserve current selection if still visible; otherwise clear
    if (selectedKey) {
      const el = rowElements.get(selectedKey);
      if (el && !el.classList.contains("mode-hidden")) {
        selectRow(selectedKey);
      } else {
        clearSelection();
      }
    } else if (selectedGroup) {
      if (selectedGroup.querySelector(".row:not(.mode-hidden)")) {
        selectGroup(selectedGroup);
      } else {
        clearSelection();
      }
    }
    scheduleLayout();
  }

  container.dataset.mode = "all";
  document.querySelectorAll(".mode-btn").forEach(btn => {
    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      document.querySelectorAll(".mode-btn").forEach(b => b.classList.toggle("active", b === btn));
      container.dataset.mode = btn.dataset.mode;
      applyMode();
    });
  });

  document.querySelectorAll(".tbl-btn:not([disabled])").forEach(btn => {
    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const t = btn.dataset.tbl;
      tableVisible[t] = !tableVisible[t];
      btn.classList.toggle("active", tableVisible[t]);
      updateGridLayout();
      applyMode();
    });
  });

  container.addEventListener("click", (ev) => {
    if (ev.target.closest(".info-area")) return;
    const header = ev.target.closest(".group-header");
    if (header) {
      const groupEl = header.parentElement;
      if (selectedGroup === groupEl) clearSelection();
      else selectGroup(groupEl);
      return;
    }
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
  updateGridLayout();
  requestAnimationFrame(() => {
    applyMode();
    setTimeout(layoutLines, 60);
    setTimeout(layoutLines, 300);
    setTimeout(syncFrameHeight, 60);
    setTimeout(syncFrameHeight, 300);
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


if __name__ == "__main__":
    app.run()
