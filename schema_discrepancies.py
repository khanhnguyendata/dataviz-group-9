"""Board vs journalist schema discrepancies — interactive Marimo notebook."""

import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    from textwrap import dedent

    mo.md(
        dedent(
            r"""
            # Table schema with dataset discrepancies

            Compare **board** data (`data/Collected_by_the_Government/`) with **journalist** data (`data/Collected_by_the_Journalist/`).

            This diagram shows **people**, **plans**, and **places** only (clearer layout). **Discussions** and **topics** are omitted here and can be summarized separately later.

            **Row / relationship encoding**

            | Fill | Meaning |
            |------|---------|
            | Light (both) | Present in **both** datasets |
            | Green | Only in the **board** export |
            | Red | Only in the **journalist** export |

            Edges between tables use the same logic at the **foreign-key tuple** level (junction CSVs). **Travel** is not a separate table: the **people ↔ places** link counts pairs that appear together on the same `trip_id` (join of `trip_people.csv` and `trip_places.csv` within each dataset).
            """
        ).strip()
    )
    return


@app.cell
def __():
    from pathlib import Path

    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    import pandas as pd
    from matplotlib import patheffects as pe
    from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

    ROOT = Path(__file__).resolve().parent
    BOARD = ROOT / "data" / "Collected_by_the_Government"
    JOURNAL = ROOT / "data" / "Collected_by_the_Journalist"

    COLOR_BOTH_FACE = "#f8fafc"
    COLOR_BOARD = "#22c55e"
    COLOR_JOURNAL = "#ef4444"
    EDGE_BOTH = "#94a3b8"
    EDGE_FONT = 7
    NODE_FONT = 7
    PANEL_LINE_H = 0.034
    PANEL_HEADER_H = 0.072
    PANEL_PAD = 0.05
    PANEL_W = 0.64
    CHAR_W_PANEL = 38

    TABLES = ["people", "plans", "places"]
    DIAGRAM_TABLES = frozenset(TABLES)

    def read_csv(path: Path) -> pd.DataFrame:
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    def classify_by_pk(
        pk: str, board_df: pd.DataFrame, journal_df: pd.DataFrame
    ) -> tuple[set[str], set[str], set[str]]:
        b = set(board_df[pk].astype(str).str.strip())
        j = set(journal_df[pk].astype(str).str.strip())
        both = b & j
        return both, b - j, j - b

    def label_row_pk(
        pk: str,
        row: pd.Series,
        both: set[str],
        b_only: set[str],
        j_only: set[str],
        side: str,
    ) -> str:
        k = str(row[pk]).strip()
        if k in both:
            return "both"
        if side == "board":
            return "board_only" if k in b_only else "journal_only"
        return "journal_only" if k in j_only else "board_only"

    def display_name(table: str, row: pd.Series) -> str:
        if table == "people":
            return str(row["name"])
        if table == "places":
            return str(row["name"])
        if table == "plans":
            st = str(row["short_title"])
            return st[:CHAR_W_PANEL] + ("…" if len(st) > CHAR_W_PANEL else "")
        return "?"

    def unique_panel_lines(
        table: str,
        pk_col: str,
        both_set: set,
        board_only: set,
        journal_only: set,
        df_b: pd.DataFrame,
        df_j: pd.DataFrame,
    ) -> list[tuple[str, str]]:
        """One line per entity (deduped across board/journal); (display_text, membership_label)."""
        universe = both_set | board_only | journal_only

        def sort_key_str(k):
            return str(k)

        lines = []
        for k in sorted(universe, key=sort_key_str):
            if k in both_set:
                lab = "both"
            elif k in board_only:
                lab = "board_only"
            else:
                lab = "journal_only"
            sk = str(k).strip()
            rb = df_b[df_b[pk_col].astype(str).str.strip() == sk]
            rj = df_j[df_j[pk_col].astype(str).str.strip() == sk]
            row = rb.iloc[0] if len(rb) else rj.iloc[0]
            lines.append((display_name(table, row), lab))
        return lines

    def color_for_label(lab: str) -> str:
        if lab == "both":
            return COLOR_BOTH_FACE
        if lab == "board_only":
            return COLOR_BOARD
        return COLOR_JOURNAL

    def text_color_for_label(lab: str) -> str:
        if lab == "both":
            return "#0f172a"
        return "#fafafa"

    # --- entity tables ---
    people_b = read_csv(BOARD / "people.csv")
    people_j = read_csv(JOURNAL / "people.csv")
    plans_b = read_csv(BOARD / "plans.csv")
    plans_j = read_csv(JOURNAL / "plans.csv")
    places_b = read_csv(BOARD / "places.csv")
    places_j = read_csv(JOURNAL / "places.csv")

    pb, p_only_b, p_only_j = classify_by_pk("people_id", people_b, people_j)
    people_b = people_b.copy()
    people_j = people_j.copy()
    people_b["_table"] = "people"
    people_j["_table"] = "people"
    people_b["_label"] = people_b.apply(
        lambda r: label_row_pk("people_id", r, pb, p_only_b, p_only_j, "board"), axis=1
    )
    people_j["_label"] = people_j.apply(
        lambda r: label_row_pk("people_id", r, pb, p_only_b, p_only_j, "journalist"), axis=1
    )

    plb, pl_only_b, pl_only_j = classify_by_pk("plan_id", plans_b, plans_j)
    plans_b = plans_b.copy()
    plans_j = plans_j.copy()
    plans_b["_table"] = "plans"
    plans_j["_table"] = "plans"
    plans_b["_label"] = plans_b.apply(
        lambda r: label_row_pk("plan_id", r, plb, pl_only_b, pl_only_j, "board"), axis=1
    )
    plans_j["_label"] = plans_j.apply(
        lambda r: label_row_pk("plan_id", r, plb, pl_only_b, pl_only_j, "journalist"), axis=1
    )

    plab, pla_only_b, pla_only_j = classify_by_pk("place_id", places_b, places_j)
    places_b = places_b.copy()
    places_j = places_j.copy()
    places_b["_table"] = "places"
    places_j["_table"] = "places"
    places_b["_label"] = places_b.apply(
        lambda r: label_row_pk("place_id", r, plab, pla_only_b, pla_only_j, "board"), axis=1
    )
    places_j["_label"] = places_j.apply(
        lambda r: label_row_pk("place_id", r, plab, pla_only_b, pla_only_j, "journalist"), axis=1
    )

    def canonical_edge(table_a: str, id_a: str, table_b: str, id_b: str) -> tuple[str, str, str, str]:
        """Lexicographic table order for stable (undirected) tuple keys."""
        sa = str(id_a).strip()
        sb = str(id_b).strip()
        if table_a <= table_b:
            return (table_a, table_b, sa, sb)
        return (table_b, table_a, sb, sa)

    def junction_row_to_canon(fname: str, row: pd.Series) -> tuple[str, str, str, str] | None:
        if fname == "discussion_topics.csv":
            return canonical_edge(
                "discussions", str(row["discussion_id"]).strip(), "topics", str(row["topic_id"]).strip()
            )
        if fname == "plan_topics.csv":
            return canonical_edge("plans", str(row["plan_id"]).strip(), "topics", str(row["topic_id"]).strip())
        if fname == "discussion_plans.csv":
            return canonical_edge(
                "discussions",
                str(row["discussion_id"]).strip(),
                "plans",
                str(row["plan_id"]).strip(),
            )
        if fname == "refers_to.csv":
            return canonical_edge(
                "discussions", str(row["discussion_id"]).strip(), "places", str(row["place_id"]).strip()
            )
        if fname == "travel_links.csv":
            return canonical_edge("plans", str(row["plan_id"]).strip(), "places", str(row["place_id"]).strip())
        if fname == "discussion_people_participations.csv":
            return canonical_edge(
                "discussions",
                str(row["discussion_id"]).strip(),
                "people",
                str(row["people_id"]).strip(),
            )
        if fname == "plan_people_participations.csv":
            return canonical_edge("plans", str(row["plan_id"]).strip(), "people", str(row["people_id"]).strip())
        return None

    JUNCTION_FILES = [
        "discussion_topics.csv",
        "plan_topics.csv",
        "discussion_plans.csv",
        "refers_to.csv",
        "travel_links.csv",
        "discussion_people_participations.csv",
        "plan_people_participations.csv",
    ]

    def people_places_via_trips(folder: Path) -> set[tuple[str, str, str, str]]:
        """(people, places) pairs that co-occur on the same trip_id in this export."""
        tp = read_csv(folder / "trip_people.csv")
        tpl = read_csv(folder / "trip_places.csv")
        merged = tp.merge(tpl, on="trip_id", how="inner", suffixes=("_tp", "_tpl"))
        out: set[tuple[str, str, str, str]] = set()
        for _, row in merged.iterrows():
            out.add(
                canonical_edge(
                    "people",
                    str(row["people_id"]).strip(),
                    "places",
                    str(row["place_id"]).strip(),
                )
            )
        return out

    def collect_junction(folder: Path) -> set[tuple[str, str, str, str]]:
        acc: set[tuple[str, str, str, str]] = set()
        for fname in JUNCTION_FILES:
            df = read_csv(folder / fname)
            for _, row in df.iterrows():
                c = junction_row_to_canon(fname, row)
                if c is not None:
                    acc.add(c)
        acc |= people_places_via_trips(folder)
        return acc

    set_b = collect_junction(BOARD)
    set_j = collect_junction(JOURNAL)
    inter = set_b & set_j
    only_b = set_b - set_j
    only_j = set_j - set_b

    # Aggregate counts per undirected table pair
    def pair_key(canon: tuple[str, str, str, str]) -> tuple[str, str]:
        return (canon[0], canon[1])

    edge_pair_stats: dict[tuple[str, str], dict[str, int]] = {}
    for c in inter:
        pk = pair_key(c)
        edge_pair_stats.setdefault(pk, {"both": 0, "board_only": 0, "journal_only": 0})["both"] += 1
    for c in only_b:
        pk = pair_key(c)
        edge_pair_stats.setdefault(pk, {"both": 0, "board_only": 0, "journal_only": 0})["board_only"] += 1
    for c in only_j:
        pk = pair_key(c)
        edge_pair_stats.setdefault(pk, {"both": 0, "board_only": 0, "journal_only": 0})["journal_only"] += 1

    entities_combined = pd.concat(
        [
            people_b.assign(_source="board"),
            people_j.assign(_source="journalist"),
            plans_b.assign(_source="board"),
            plans_j.assign(_source="journalist"),
            places_b.assign(_source="board"),
            places_j.assign(_source="journalist"),
        ],
        ignore_index=True,
    )
    entities_combined["_display"] = entities_combined.apply(
        lambda r: display_name(str(r["_table"]), r), axis=1
    )

    def entity_key_row(r: pd.Series) -> str:
        t = str(r["_table"])
        col = {"people": "people_id", "plans": "plan_id", "places": "place_id"}[t]
        return str(r[col]).strip()

    entities_combined["_entity_key"] = entities_combined.apply(entity_key_row, axis=1)
    entities_view = (
        entities_combined[
            ["_entity_key", "_table", "_display", "_label", "_source"]
        ]
        .rename(
            columns={
                "_entity_key": "id",
                "_table": "table",
                "_display": "label",
                "_label": "membership",
                "_source": "dataset",
            }
        )
        .assign(_prio=lambda d: d["dataset"].map({"board": 0, "journalist": 1}))
        .sort_values(["table", "id", "_prio"], kind="mergesort")
        .drop_duplicates(["table", "id"], keep="first")
        .drop(columns=["_prio", "dataset"])
    )

    panel_lines_by_table = {
        "people": unique_panel_lines(
            "people", "people_id", pb, p_only_b, p_only_j, people_b, people_j
        ),
        "plans": unique_panel_lines(
            "plans", "plan_id", plb, pl_only_b, pl_only_j, plans_b, plans_j
        ),
        "places": unique_panel_lines(
            "places", "place_id", plab, pla_only_b, pla_only_j, places_b, places_j
        ),
    }
    _max_panel_rows = max(len(v) for v in panel_lines_by_table.values())

    # --- layout: triangle (people at top), radius scales when panels are tall ---
    def table_positions(radius: float) -> dict[str, tuple[float, float]]:
        pos: dict[str, tuple[float, float]] = {}
        order = ["people", "plans", "places"]
        n = len(order)
        for i, name in enumerate(order):
            ang = np.pi / 2 - 2 * np.pi * i / n
            pos[name] = (float(radius * np.cos(ang)), float(radius * np.sin(ang)))
        return pos

    def plot_schema() -> plt.Figure:
        spread = 1.15 + min(0.45, 0.006 * _max_panel_rows)
        pos = table_positions(spread)
        fig, ax = plt.subplots(figsize=(14, 13), dpi=110)
        lim = spread + PANEL_W * 0.75 + 0.35
        ax.set_xlim(-lim, lim)
        ax.set_ylim(-lim, lim)
        ax.set_aspect("equal")
        ax.axis("off")
        ax.set_facecolor("#f1f5f9")
        fig.patch.set_facecolor("#f1f5f9")

        # Draw edges only between the three diagram tables (people, plans, places)
        for (ta, tb), stats in sorted(edge_pair_stats.items()):
            if ta not in DIAGRAM_TABLES or tb not in DIAGRAM_TABLES:
                continue
            if ta not in pos or tb not in pos:
                continue
            x1, y1 = pos[ta]
            x2, y2 = pos[tb]
            nb, nbo, nj = stats["both"], stats["board_only"], stats["journal_only"]
            label = f"↔{nb}  B{nbo}  J{nj}"

            def draw_arc(color: str, rad: float, lw: float) -> None:
                patch = FancyArrowPatch(
                    (x1, y1),
                    (x2, y2),
                    connectionstyle=f"arc3,rad={rad}",
                    arrowstyle="-",
                    color=color,
                    linewidth=lw,
                    alpha=0.9,
                    zorder=1,
                )
                ax.add_patch(patch)

            if nb > 0 and nbo == 0 and nj == 0:
                draw_arc(EDGE_BOTH, 0.12, 3.0)
            elif nb > 0:
                draw_arc(EDGE_BOTH, 0.12, 3.0)
                if nbo > 0:
                    draw_arc(COLOR_BOARD, 0.22, 2.0)
                if nj > 0:
                    draw_arc(COLOR_JOURNAL, -0.22, 2.0)
            elif nbo > 0 and nj > 0:
                draw_arc(COLOR_BOARD, 0.18, 2.5)
                draw_arc(COLOR_JOURNAL, -0.18, 2.5)
            elif nbo > 0:
                draw_arc(COLOR_BOARD, 0.12, 3.0)
            elif nj > 0:
                draw_arc(COLOR_JOURNAL, 0.12, 3.0)

            mx, my = (x1 + x2) / 2, (y1 + y2) / 2
            off = 0.08 * (1 if (x2 - x1) >= 0 else -1)
            ax.text(
                mx + off,
                my + off,
                label,
                ha="center",
                va="center",
                fontsize=EDGE_FONT,
                color="#334155",
                zorder=2,
                path_effects=[pe.withStroke(linewidth=2, foreground="#f1f5f9")],
            )

        # Table panels: one row per entity (deduped); height fits all rows
        box_w = PANEL_W

        for t in TABLES:
            x, y = pos[t]
            all_lines = panel_lines_by_table[t]
            n = max(1, len(all_lines))
            box_h = PANEL_HEADER_H + n * PANEL_LINE_H + PANEL_PAD
            bx0 = x - box_w / 2
            by0 = y - box_h / 2
            rect = FancyBboxPatch(
                (bx0, by0),
                box_w,
                box_h,
                boxstyle="round,pad=0.02,rounding_size=0.04",
                facecolor="white",
                edgecolor="#64748b",
                linewidth=1.2,
                zorder=3,
            )
            ax.add_patch(rect)

            ax.text(
                x,
                by0 + box_h - 0.04,
                f"{t} ({n})",
                ha="center",
                va="top",
                fontsize=11,
                fontweight="bold",
                color="#0f172a",
                zorder=4,
            )

            y_text = by0 + box_h - PANEL_HEADER_H + 0.01
            for txt, lab in all_lines:
                col = color_for_label(lab)
                tc = text_color_for_label(lab)
                ax.text(
                    bx0 + 0.025,
                    y_text,
                    txt[:CHAR_W_PANEL] + ("…" if len(txt) > CHAR_W_PANEL else ""),
                    ha="left",
                    va="top",
                    fontsize=NODE_FONT,
                    color=tc,
                    bbox=dict(
                        boxstyle="round,pad=0.12",
                        facecolor=col,
                        edgecolor="#94a3b8",
                        linewidth=0.35,
                    ),
                    zorder=5,
                )
                y_text -= PANEL_LINE_H

        # Legend
        leg_y = -lim + 0.14
        ax.text(
            -lim + 0.05,
            leg_y,
            "Rows: light = in both datasets; green = board only; red = journalist only.",
            fontsize=9,
            color="#334155",
        )
        ax.text(
            -lim + 0.05,
            leg_y - 0.07,
            "Edges: gray = ≥1 identical FK tuple in both; people↔places includes co-travel (same trip_id). Labels: ↔ shared, B board-only, J journalist-only.",
            fontsize=9,
            color="#334155",
        )
        patches = [
            mpatches.Patch(facecolor=COLOR_BOTH_FACE, edgecolor="#94a3b8", label="Both datasets"),
            mpatches.Patch(facecolor=COLOR_BOARD, edgecolor="#15803d", label="Board only"),
            mpatches.Patch(facecolor=COLOR_JOURNAL, edgecolor="#b91c1c", label="Journalist only"),
        ]
        ax.legend(
            handles=patches,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.02),
            ncol=3,
            frameon=True,
            fontsize=9,
        )

        fig.subplots_adjust(left=0.03, right=0.97, top=0.97, bottom=0.06)
        return fig

    figure = plot_schema()

    edge_summary_df = pd.DataFrame(
        [
            {"table_a": a, "table_b": b, **stats}
            for (a, b), stats in sorted(edge_pair_stats.items())
            if a in DIAGRAM_TABLES and b in DIAGRAM_TABLES
        ]
    )

    return (
        mo,
        pd,
        entities_view,
        edge_summary_df,
        figure,
    )


@app.cell
def __(mo, figure):
    mo.mpl.interactive(figure)
    return


@app.cell
def __(mo, pd, entities_view):
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
def __(mo, pd, edge_summary_df):
    mo.vstack(
        [
            mo.md(
                "## Relationship counts (edges among **people**, **plans**, **places** only; **people↔places** includes same `trip_id` in `trip_people` & `trip_places`)"
            ),
            mo.ui.dataframe(edge_summary_df, page_size=20),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
