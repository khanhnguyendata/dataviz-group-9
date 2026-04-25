import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import altair as alt
    # Show ALL rows and columns, full content in each cell
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    return alt, mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Load data
    """)
    return


@app.cell
def _(pd):
    # ── 1. Load all CSVs ──────────────────────────────────────────────────────────
    GOV_PATH = "data/Collected_by_the_Government/"

    gov_disc_org_part       = pd.read_csv(GOV_PATH + "discussion_org_participations.csv")
    gov_disc_people_part    = pd.read_csv(GOV_PATH + "discussion_people_participations.csv")
    gov_discussion_plans    = pd.read_csv(GOV_PATH + "discussion_plans.csv")
    gov_discussion_topics   = pd.read_csv(GOV_PATH + "discussion_topics.csv")
    gov_discussions         = pd.read_csv(GOV_PATH + "discussions.csv")

    gov_meeting_discussions = pd.read_csv(GOV_PATH + "meeting_discussions.csv")
    gov_meeting_plans       = pd.read_csv(GOV_PATH + "meeting_plans.csv")
    gov_meetings            = pd.read_csv(GOV_PATH + "meetings.csv")
    gov_organizations       = pd.read_csv(GOV_PATH + "organizations.csv")
    gov_people              = pd.read_csv(GOV_PATH + "people.csv")
    gov_places              = pd.read_csv(GOV_PATH + "places.csv")

    gov_plan_org_part       = pd.read_csv(GOV_PATH + "plan_org_participations.csv")
    gov_plan_people_part    = pd.read_csv(GOV_PATH + "plan_people_participations.csv")
    gov_plan_topics         = pd.read_csv(GOV_PATH + "plan_topics.csv")
    gov_plans               = pd.read_csv(GOV_PATH + "plans.csv")
    gov_refers_to           = pd.read_csv(GOV_PATH + "refers_to.csv")
    gov_topics              = pd.read_csv(GOV_PATH + "topics.csv")
    gov_travel_links        = pd.read_csv(GOV_PATH + "travel_links.csv")
    gov_trip_people         = pd.read_csv(GOV_PATH + "trip_people.csv")
    gov_trip_places         = pd.read_csv(GOV_PATH + "trip_places.csv")
    gov_trips               = pd.read_csv(GOV_PATH + "trips.csv")

    # ── 2. Rename ambiguous columns before merging ────────────────────────────────
    gov_places              = gov_places.rename(columns={"name": "place_name"})
    gov_people              = gov_people.rename(columns={"name": "person_name"})
    gov_organizations       = gov_organizations.rename(columns={"name": "org_name"})
    gov_trip_places         = gov_trip_places.rename(columns={"time": "trip_place_time"})
    gov_trip_people         = gov_trip_people.rename(columns={"time": "trip_person_time"})
    gov_meetings            = gov_meetings.rename(columns={"date": "meeting_date", "label": "meeting_label"})
    gov_trips               = gov_trips.rename(columns={"date": "trip_date"})
    gov_plans               = gov_plans.rename(columns={"short_title": "plan_short_title", "long_title": "plan_long_title"})
    gov_discussions         = gov_discussions.rename(columns={"short_title": "disc_short_title", "long_title": "disc_long_title"})
    gov_plan_people_part    = gov_plan_people_part.rename(columns={"sentiment": "plan_person_sentiment", "reason": "plan_person_reason"})
    gov_plan_org_part       = gov_plan_org_part.rename(columns={"sentiment": "plan_org_sentiment"})
    gov_disc_people_part    = gov_disc_people_part.rename(columns={"sentiment": "disc_person_sentiment", "reason": "disc_person_reason"})
    gov_disc_org_part       = gov_disc_org_part.rename(columns={"sentiment": "disc_org_sentiment"})
    gov_plan_topics         = gov_plan_topics.rename(columns={"topic_id": "plan_topic_id"})
    gov_discussion_topics   = gov_discussion_topics.rename(columns={"topic_id": "disc_topic_id", "status": "disc_topic_status"})
    gov_refers_to           = gov_refers_to.rename(columns={"place_id": "referred_place_id"})

    gov_disc_org_part      = gov_disc_org_part.rename(columns={"industry": "industry_disc_org_part"})
    return gov_disc_people_part, gov_discussion_topics, gov_people, gov_topics


@app.cell
def _(pd):
    # ── 1. Load all CSVs ──────────────────────────────────────────────────────────
    JOURNALIST_PATH = "data/Collected_by_the_Journalist/"

    jnl_disc_org_part       = pd.read_csv(JOURNALIST_PATH + "discussion_org_participations.csv")
    jnl_disc_people_part    = pd.read_csv(JOURNALIST_PATH + "discussion_people_participations.csv")
    jnl_discussion_plans    = pd.read_csv(JOURNALIST_PATH + "discussion_plans.csv")
    jnl_discussion_topics   = pd.read_csv(JOURNALIST_PATH + "discussion_topics.csv")
    jnl_discussions        = pd.read_csv(JOURNALIST_PATH + "discussions.csv")

    jnl_meeting_discussions = pd.read_csv(JOURNALIST_PATH + "meeting_discussions.csv")
    jnl_meeting_plans       = pd.read_csv(JOURNALIST_PATH + "meeting_plans.csv")
    jnl_meetings            = pd.read_csv(JOURNALIST_PATH + "meetings.csv")
    jnl_organizations       = pd.read_csv(JOURNALIST_PATH + "organizations.csv")
    jnl_people              = pd.read_csv(JOURNALIST_PATH + "people.csv")
    jnl_places              = pd.read_csv(JOURNALIST_PATH + "places.csv")

    jnl_plan_org_part       = pd.read_csv(JOURNALIST_PATH + "plan_org_participations.csv")
    jnl_plan_people_part    = pd.read_csv(JOURNALIST_PATH + "plan_people_participations.csv")
    jnl_plan_topics         = pd.read_csv(JOURNALIST_PATH + "plan_topics.csv")
    jnl_plans               = pd.read_csv(JOURNALIST_PATH + "plans.csv")
    jnl_refers_to           = pd.read_csv(JOURNALIST_PATH + "refers_to.csv")
    jnl_topics              = pd.read_csv(JOURNALIST_PATH + "topics.csv")
    jnl_travel_links        = pd.read_csv(JOURNALIST_PATH + "travel_links.csv")
    jnl_trip_people         = pd.read_csv(JOURNALIST_PATH + "trip_people.csv")
    jnl_trip_places         = pd.read_csv(JOURNALIST_PATH + "trip_places.csv")
    jnl_trips               = pd.read_csv(JOURNALIST_PATH + "trips.csv")

    # ── 2. Rename ambiguous columns before merging ────────────────────────────────
    jnl_places              = jnl_places.rename(columns={"name": "place_name"})
    jnl_people              = jnl_people.rename(columns={"name": "person_name"})
    jnl_organizations       = jnl_organizations.rename(columns={"name": "org_name"})
    jnl_trip_places         = jnl_trip_places.rename(columns={"time": "trip_place_time"})
    jnl_trip_people         = jnl_trip_people.rename(columns={"time": "trip_person_time"})
    jnl_meetings            = jnl_meetings.rename(columns={"date": "meeting_date", "label": "meeting_label"})
    jnl_trips               = jnl_trips.rename(columns={"date": "trip_date"})
    jnl_plans               = jnl_plans.rename(columns={"short_title": "plan_short_title", "long_title": "plan_long_title"})
    jnl_discussions         = jnl_discussions.rename(columns={"short_title": "disc_short_title", "long_title": "disc_long_title"})
    jnl_plan_people_part    = jnl_plan_people_part.rename(columns={"sentiment": "plan_person_sentiment", "reason": "plan_person_reason"})
    jnl_plan_org_part       = jnl_plan_org_part.rename(columns={"sentiment": "plan_org_sentiment"})
    jnl_disc_people_part    = jnl_disc_people_part.rename(columns={"sentiment": "disc_person_sentiment", "reason": "disc_person_reason"})
    jnl_disc_org_part       = jnl_disc_org_part.rename(columns={"sentiment": "disc_org_sentiment"})
    jnl_plan_topics         = jnl_plan_topics.rename(columns={"topic_id": "plan_topic_id"})
    jnl_discussion_topics   = jnl_discussion_topics.rename(columns={"topic_id": "disc_topic_id", "status": "disc_topic_status"})
    jnl_refers_to           = jnl_refers_to.rename(columns={"place_id": "referred_place_id"})

    jnl_disc_org_part      = jnl_disc_org_part.rename(columns={"industry": "industry_disc_org_part"})
    return jnl_disc_people_part, jnl_discussion_topics, jnl_people, jnl_topics


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Data transformation
    """)
    return


@app.cell
def _(
    gov_disc_people_part,
    gov_discussion_topics,
    gov_people,
    gov_topics,
    jnl_disc_people_part,
    jnl_discussion_topics,
    jnl_people,
    jnl_topics,
    pd,
):
    # define industry category mapping rule
    industry_cate1 = {
        "['large vessel', 'small vessel']":       "fishing",
        "['large vessel']":                       "fishing",
        "['small vessel']":                       "fishing",
        "['tourism', 'small vessel']":            "mixed",
        "['tourism']":                            "tourism"
    }
    industry_cate2 = {
        "renaming_park_himark":       "tourism",
        "name_harbor_area":           "tourism",
        "name_inspection_office":     "tourism",
        "statue_john_smoth":          "tourism",
    }

    # transform journalist data
    jnl_disc = (
        jnl_disc_people_part                                        # discussion_id, people_id, sentiment, reason
        .merge(jnl_discussion_topics, on="discussion_id", how="left")   # + disc_topic_id, disc_topic_status
        .merge(jnl_topics, left_on="disc_topic_id", right_on="topic_id", how="left")  # + short_topic, category
        .merge(jnl_people, on="people_id", how="left")             # + person_name, role
    )

    jnl_disc["industry_category"] = jnl_disc["industry"].map(industry_cate1).fillna(
        jnl_disc["short_topic"].map(industry_cate2)
    )
    jnl_disc[["source"]] = "journalist"

    # transform government data
    gov_disc = (
        gov_disc_people_part                                        # discussion_id, people_id, sentiment, reason
        .merge(gov_discussion_topics, on="discussion_id", how="left")   # + disc_topic_id, disc_topic_status
        .merge(gov_topics, left_on="disc_topic_id", right_on="topic_id", how="left")  # + short_topic, category
        .merge(gov_people, on="people_id", how="left")             # + person_name, role
    )

    gov_disc["industry_category"] = gov_disc["industry"].map(industry_cate1).fillna(
        gov_disc["short_topic"].map(industry_cate2)
    )
    gov_disc[["source"]] = "board"

    # concat data from two sources
    df = pd.concat([jnl_disc, gov_disc], ignore_index=True)[["source", "industry_category", "disc_topic_id", "discussion_id", "disc_person_reason", "person_name", "role", "disc_person_sentiment"]]
    df["disc_topic_id"] = df["disc_topic_id"].str.replace("_", " ", regex=False).str.capitalize()

    # create pivotal dataset
    topic_agg = (df
        .groupby(["source", "industry_category", "disc_topic_id"])
        .agg(avg_sentiment=('disc_person_sentiment', 'mean'), disc_cnt=('discussion_id', 'nunique'))
        .reset_index()
                )

    topic_agg[["type"]] = "Discussion topics"
    topic_agg.rename(columns={"disc_topic_id": "label", "disc_person_sentiment": "sentiment"}, inplace=True)

    member_agg = (df
        .groupby(["source", "industry_category", "person_name"])
        .agg(avg_sentiment=('disc_person_sentiment', 'mean'), disc_cnt=('discussion_id', 'nunique'))
        .reset_index()
                )
    member_agg[["type"]] = "Board members"
    member_agg.rename(columns={"person_name": "label", "disc_person_sentiment": "sentiment"}, inplace=True)

    df_agg = pd.concat([topic_agg, member_agg], ignore_index=True)
    return df, df_agg


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Visualization
    """)
    return


@app.cell
def _(alt, df, df_agg, mo):
    # ── Cell 2: constants ─────────────────────────────────────────────────────────
    BG_SINGLE      = "#E8EDF7"
    BG_OVERLAPPING = "#FFF0E0"
    SOURCE_DOMAIN  = ["board", "journalist"]
    SOURCE_RANGE   = ["#000080", "#CD5C5C"]
    CAT_ORDER      = ["fishing", "mixed", "tourism"]
    CAT_COLORS     = {"fishing": "#1565c0", "mixed": "#6a1b9a", "tourism": "#e65100"}

    # ── Cell 3: shared encodings ──────────────────────────────────────────────────
    color = alt.Color(
        "source:N",
        scale=alt.Scale(domain=SOURCE_DOMAIN, range=SOURCE_RANGE),
        legend=alt.Legend(title="Data sources", clipHeight=30),
    )
    cat_color = alt.Color(
        "industry_category:N",
        scale=alt.Scale(
            domain=list(CAT_COLORS.keys()),
            range=list(CAT_COLORS.values()),
        ),
        legend=alt.Legend(title="Categories", orient="right"),  # ← shown in bg only
    )

    cat_color_no_legend = alt.Color(
        "industry_category:N",
        scale=alt.Scale(
            domain=list(CAT_COLORS.keys()),
            range=list(CAT_COLORS.values()),
        ),
        legend=None, 
    )

    source_color = alt.Color(
        "source:N",
        scale=alt.Scale(domain=SOURCE_DOMAIN, range=SOURCE_RANGE),
        legend=alt.Legend(title="Data sources", orient="right"),    # ← shown in dots only
    )

    source_color_no_legend = alt.Color(
        "source:N",
        scale=alt.Scale(domain=SOURCE_DOMAIN, range=SOURCE_RANGE),
        legend=None,   # ← for v_lines, avg_text, etc.
    )
    x_scale = alt.Scale(domain=[-1, 1])

    # ── Cell 4: Marimo toggle ─────────────────────────────────────────────────────
    view_toggle = mo.ui.radio(
        options=["Discussion topics", "Board members"],
        value="Discussion topics",
        label="View primary visualization by:",
    )

    # ── Cell 5: make_lane ─────────────────────────────────────────────────────────
    def make_lane(category, df_filtered):
        df_wide = (
            df_filtered
            .pivot_table(index=["type","label"], columns="source", values="avg_sentiment")
            .reset_index()
        )
        df_wide.columns.name = None
        for col in ["board", "journalist"]:
            if col not in df_wide.columns:
                df_wide[col] = float("nan")
        df_wide["has_both"] = df_wide[["journalist","board"]].notna().all(axis=1)

        df_avg = (
            df_filtered
            .groupby("source")["avg_sentiment"]
            .mean().reset_index()
            .rename(columns={"avg_sentiment": "cat_avg"})
        )

        bg = (
            alt.Chart(df_wide)
            .mark_rect(opacity=0.4)
            .encode(
                y=alt.Y("label:N", title=None,
                        axis=alt.Axis(labelFontSize=12, domain=False, ticks=False, offset=10)),
                color=alt.condition(
                    "datum.has_both",
                    alt.value(BG_OVERLAPPING),
                    alt.value(BG_SINGLE),
                ),
            )
        )

        horizontal_lines = (
            alt.Chart(df_wide)
            .mark_rule(strokeWidth=1.5, opacity=0.5, color="gray")
            .encode(
                x=alt.X("journalist:Q", scale=x_scale),
                x2="board:Q",
                y=alt.Y("label:N"),
                tooltip=[
                    "label:N",
                    alt.Tooltip("journalist:Q", title="Journalist:", format=".3f"),
                    alt.Tooltip("board:Q",      title="Board:",      format=".3f"),
                ],
            )
        )

        dots = (
            alt.Chart(df_filtered)
            .mark_circle(stroke="black", strokeWidth=1, strokeOpacity=0.4, size=80)
            .encode(
                alt.X("avg_sentiment:Q")
                    .title("Average sentiment" if category == "tourism" else "")
                    .scale(x_scale),
                alt.Y("label:N")
                    .title(None)
                    .axis(alt.Axis(labelFontSize=12, domain=False, ticks=False, offset=10)),
                alt.Size("disc_cnt:Q")
                    .scale(domainMin=1)
                    .title("# Discussions")
                    .legend(clipHeight=30, format="d"),
                color,
                tooltip=[
                    alt.Tooltip("source:N",        title="Data source:"),
                    alt.Tooltip("type_label:N",     title="Granularity:"),
                    alt.Tooltip("avg_sentiment:Q",  title="Avg sentiment:", format=".1f"),
                    alt.Tooltip("disc_cnt:Q",       title="# discussions:", format="~s"),
                ],
            )
            .transform_calculate(type_label="datum.type + ': ' + datum.label")
        )

        vertical_lines = (
            alt.Chart(df_avg)
            .mark_rule(strokeWidth=2, opacity=0.9, strokeDash=[4, 2])
            .encode(
                x=alt.X("cat_avg:Q", scale=x_scale),
                color=color,
                tooltip=[
                    alt.Tooltip("cat_avg:Q", title="Category avg:", format=".1f"),
                    "source:N",
                ],
            )
        )

        avg_text = (
            alt.Chart(df_avg)
            .mark_text(dy=-8, dx=12, fontSize=11, fontWeight="bold")
            .encode(
                x=alt.X("cat_avg:Q", scale=x_scale),
                text=alt.Text("cat_avg:Q", format=".1f"),
                color=color,
            )
        )

        return (
            alt.layer(bg, horizontal_lines, dots, vertical_lines, avg_text)
            .properties(
                width=520,
                height=alt.Step(32),
                title=alt.Title(
                    text=(
                    [f"Average sentiment by Discussion category and {view_toggle.value}", category.upper()]
                    if category == "fishing"
                    else [category.upper()]
        ),
                    anchor="start",
                    fontSize=13,
                    fontWeight="bold",
                    lineHeight=20,
                )
            )
        )

    # ── Cell 6: make_chart ────────────────────────────────────────────────────────
    def make_chart(view_type):
        df_filtered = df_agg[df_agg["type"] == view_type].copy()
        lanes = [
            make_lane(cat, df_filtered[df_filtered["industry_category"] == cat])
            for cat in CAT_ORDER
            if cat in df_filtered["industry_category"].values
        ]
        return (
            alt.vconcat(*lanes)
            .resolve_scale(color="shared", size="shared")
        )


    # ── Cell 7: make_drill — Option B (all categories, colored by category) ───────
    # def make_drill(selected_label, current_type):
    #     drill_type = "Board members" if current_type == "Discussion topics" else "Discussion topics"

    #     if current_type == "Discussion topics":
    #         # Selected a topic → show all members who discussed it, across all categories
    #         df_drill = (
    #             df[df["disc_topic_id"] == selected_label]
    #             .groupby(["source", "industry_category", "person_name"])
    #             .agg(avg_sentiment=("disc_person_sentiment", "mean"),
    #                  disc_cnt=("discussion_id", "nunique"))
    #             .reset_index()
    #             .rename(columns={"person_name": "label"})
    #         )
    #     else:
    #         # Selected a member → show all topics they discussed, across all categories
    #         df_drill = (
    #             df[df["person_name"] == selected_label]
    #             .groupby(["source", "industry_category", "disc_topic_id"])
    #             .agg(avg_sentiment=("disc_person_sentiment", "mean"),
    #                  disc_cnt=("discussion_id", "nunique"))
    #             .reset_index()
    #             .rename(columns={"disc_topic_id": "label"})
    #         )

    #     if len(df_drill) == 0:
    #         return None

    #     # Sort y-axis: group by industry_category so fishing/mixed/tourism cluster together
    #     label_order = (
    #         df_drill
    #         .drop_duplicates("label")
    #         .sort_values(["industry_category", "label"])["label"]
    #         .tolist()
    #     )

    #     # Pivot wide for connecting lines
    #     df_drill_wide = (
    #         df_drill
    #         .pivot_table(index=["industry_category","label"], columns="source", values="avg_sentiment")
    #         .reset_index()
    #     )
    #     df_drill_wide.columns.name = None
    #     for col in ["board","journalist"]:
    #         if col not in df_drill_wide.columns:
    #             df_drill_wide[col] = float("nan")

    #     # Category avg lines
    #     df_drill_avg = (
    #         df_drill
    #         .groupby(["source","industry_category"])["avg_sentiment"]
    #         .mean().reset_index()
    #         .rename(columns={"avg_sentiment": "cat_avg"})
    #     )

    #     y_enc = alt.Y("label:N", title=None,
    #                   sort=label_order,
    #                   axis=alt.Axis(labelFontSize=12, domain=False, ticks=False, offset=10))

    #     # Background color by industry_category
    #     bg =(
    #     alt.Chart(df_drill_wide)
    #     .mark_rect(opacity=0.15)
    #     .encode(
    #         y=alt.Y("label:N", sort=label_order, title=None,
    #                 axis=alt.Axis(labelFontSize=12, domain=False, ticks=False, offset=10)),
    #         fill=alt.Fill(                         # ← fill, not color
    #             "industry_category:N",
    #             scale=alt.Scale(
    #                 domain=list(CAT_COLORS.keys()),
    #                 range=list(CAT_COLORS.values()),
    #             ),
    #             legend=alt.Legend(title="Categories"),
    #         ),
    #     )
    # )

    #     # Connecting lines
    #     h_lines = (
    #     alt.Chart(df_drill_wide)
    #     .mark_rule(strokeWidth=1.5, opacity=0.5, color="gray")
    #     .encode(
    #         x=alt.X("journalist:Q", scale=x_scale),
    #         x2="board:Q",
    #         y=alt.Y("label:N", sort=label_order, title=None),
    #         tooltip=[
    #             "label:N", "industry_category:N",
    #             alt.Tooltip("journalist:Q", title="Journalist:", format=".3f"),
    #             alt.Tooltip("board:Q",      title="Board:",      format=".3f"),
    #         ],
    #     )
    # )

    #     dots = (
    #     alt.Chart(df_drill)
    #     .mark_circle(stroke="black", strokeWidth=1, strokeOpacity=0.4, size=80)
    #     .encode(
    #         x=alt.X("avg_sentiment:Q").title("Average sentiment").scale(x_scale),
    #         y=alt.Y("label:N", sort=label_order, title=None,
    #                 axis=alt.Axis(labelFontSize=12, domain=False, ticks=False, offset=10)),
    #         color=source_color,                # ← shows source legend
    #         size=alt.Size(
    #             "disc_cnt:Q",
    #             title="# Discussions",
    #             scale=alt.Scale(domainMin=1),
    #             legend=alt.Legend(clipHeight=30, format="d", values=[1, 2, 4, 6, 8, 10]),
    #         ),
    #         tooltip=[
    #             alt.Tooltip("label:N",             title="Label:"),
    #             alt.Tooltip("source:N",            title="Source:"),
    #             alt.Tooltip("industry_category:N", title="Category:"),
    #             alt.Tooltip("avg_sentiment:Q",     title="Avg sentiment:", format=".1f"),
    #             alt.Tooltip("disc_cnt:Q",          title="# discussions:"),
    #         ],
    #     )
    # )

    #     # Vertical avg lines per source per category
    #     v_lines = (
    #     alt.Chart(df_drill_avg)
    #     .mark_rule(strokeWidth=2, opacity=0.7, strokeDash=[4, 2])
    #     .encode(
    #         x=alt.X("cat_avg:Q", scale=x_scale),
    #         color=source_color_no_legend,      # ← no legend
    #         tooltip=[
    #             alt.Tooltip("cat_avg:Q",           title="Category avg:", format=".1f"),
    #             "source:N", "industry_category:N",
    #         ],
    #     )
    # )

    #     return (
    #     alt.layer(bg, h_lines, dots, v_lines)
    #     .resolve_legend(color="independent", size="independent")
    #     .properties(
    #             width=520,
    #             height=alt.Step(32),
    #             title=alt.Title(
    #                 text=f"{selected_label}  —  breakdown by {drill_type} (all categories)",
    #                 anchor="start", fontSize=13, fontWeight="bold",
    #             )
    #         )
    #     )
    def make_drill(selected_label, current_type):
        drill_type = "Board members" if current_type == "Discussion topics" else "Discussion topics"

        # ── Build drill DataFrame ─────────────────────────────────────────────────
        if current_type == "Discussion topics":
            df_drill = (
                df[df["disc_topic_id"] == selected_label]
                .groupby(["source", "industry_category", "person_name"])
                .agg(avg_sentiment=("disc_person_sentiment", "mean"),
                     disc_cnt=("discussion_id", "nunique"))
                .reset_index()
                .rename(columns={"person_name": "label"})
            )
        else:
            df_drill = (
                df[df["person_name"] == selected_label]
                .groupby(["source", "industry_category", "disc_topic_id"])
                .agg(avg_sentiment=("disc_person_sentiment", "mean"),
                     disc_cnt=("discussion_id", "nunique"))
                .reset_index()
                .rename(columns={"disc_topic_id": "label"})
            )

        if len(df_drill) == 0:
            return None

        # ── One lane per category ─────────────────────────────────────────────────
        def make_drill_lane(category, df_cat):
            df_wide = (
                df_cat
                .pivot_table(index=["label"], columns="source", values="avg_sentiment")
                .reset_index()
            )
            df_wide.columns.name = None
            for col in ["board", "journalist"]:
                if col not in df_wide.columns:
                    df_wide[col] = float("nan")

            df_avg = (
                df_cat
                .groupby("source")["avg_sentiment"]
                .mean().reset_index()
                .rename(columns={"avg_sentiment": "cat_avg"})
            )

            # Background strips — same as primary chart (single vs overlapping source)
            df_wide["has_both"] = df_wide[["journalist", "board"]].notna().all(axis=1)

            bg = (
                alt.Chart(df_wide)
                .mark_rect(opacity=0.4)
                .encode(
                    y=alt.Y("label:N", title=None,
                            axis=alt.Axis(labelFontSize=12, domain=False,
                                          ticks=False, offset=10)),
                    color=alt.condition(
                        "datum.has_both",
                        alt.value(BG_OVERLAPPING),
                        alt.value(BG_SINGLE),
                    ),
                )
            )

            # Horizontal connecting lines
            h_lines = (
                alt.Chart(df_wide)
                .mark_rule(strokeWidth=1.5, opacity=0.5, color="gray")
                .encode(
                    x=alt.X("journalist:Q", scale=x_scale),
                    x2="board:Q",
                    y=alt.Y("label:N", title=None,
                            axis=alt.Axis(labelFontSize=12, domain=False,
                                          ticks=False, offset=10)),
                    tooltip=[
                        "label:N",
                        alt.Tooltip("journalist:Q", title="Journalist:", format=".3f"),
                        alt.Tooltip("board:Q",      title="Board:",      format=".3f"),
                    ],
                )
            )

            # Dots
            dots = (
                alt.Chart(df_cat)
                .mark_circle(stroke="black", strokeWidth=1, strokeOpacity=0.4, size=80)
                .encode(
                    alt.X("avg_sentiment:Q")
                        .title("Average sentiment" if category == "tourism" else "")
                        .scale(x_scale),
                    alt.Y("label:N")
                        .title(None)
                        .axis(alt.Axis(labelFontSize=12, domain=False,
                                       ticks=False, offset=10)),
                    color=source_color,
                    size=alt.Size(
                        "disc_cnt:Q",
                        title="# Discussions",
                        scale=alt.Scale(domainMin=1),
                        legend=alt.Legend(clipHeight=30, format="d",
                                          values=[1, 2, 4, 6, 8, 10]),
                    ),
                    tooltip=[
                        alt.Tooltip("label:N",             title=f"{drill_type}:"),
                        alt.Tooltip("source:N",            title="Source:"),
                        alt.Tooltip("industry_category:N", title="Category:"),
                        alt.Tooltip("avg_sentiment:Q",     title="Avg sentiment:", format=".1f"),
                        alt.Tooltip("disc_cnt:Q",          title="# discussions:"),
                    ],
                )
            )

            # Vertical avg lines
            v_lines = (
                alt.Chart(df_avg)
                .mark_rule(strokeWidth=2, opacity=0.7, strokeDash=[4, 2])
                .encode(
                    x=alt.X("cat_avg:Q", scale=x_scale),
                    color=source_color_no_legend,
                    tooltip=[
                        alt.Tooltip("cat_avg:Q", title="Category avg:", format=".1f"),
                        "source:N",
                    ],
                )
            )

            # Avg text
            avg_text = (
                alt.Chart(df_avg)
                .mark_text(dy=-8, dx=12, fontSize=11, fontWeight="bold")
                .encode(
                    x=alt.X("cat_avg:Q", scale=x_scale),
                    text=alt.Text("cat_avg:Q", format=".1f"),
                    color=source_color_no_legend,
                )
            )

            return (
                alt.layer(bg, h_lines, dots, v_lines, avg_text)
                .properties(
                    width=520,
                    height=alt.Step(32),
                    title=alt.Title(
                        text=(
                            [f"{selected_label}  —  breakdown by {drill_type}", " ", category.upper()]
                            if category == "fishing"
                            else [" ", " ", category.upper()]
                        ),
                        anchor="start",
                        fontSize=13,
                        fontWeight="bold",
                        lineHeight=20,
                    )
                )
            )

        # ── Build lanes for categories that have data ─────────────────────────────
        lanes = [
            make_drill_lane(cat, df_drill[df_drill["industry_category"] == cat])
            for cat in CAT_ORDER
            if cat in df_drill["industry_category"].values
        ]

        if not lanes:
            return None

        return (
            alt.vconcat(*lanes)
            .resolve_scale(color="shared", size="shared")
            .configure_view(stroke="lightgray")
            .configure_axisY(domain=False, ticks=False, offset=10)
            .configure_axis(labelFontSize=11, titleFontSize=12)
        )

    return make_chart, make_drill, view_toggle


@app.cell
def _(view_toggle):
    # # ── Cell 8: view toggle + dropdown ───────────────────────────────────────────
    view_toggle
    return


@app.cell
def _(make_chart, mo, view_toggle):
    # display primary chart
    mo.as_html(
        make_chart(view_toggle.value)
        .configure_view(stroke="lightgray")
        .configure_axisY(domain=False, ticks=False, offset=10)
        .configure_axis(labelFontSize=11, titleFontSize=12)
    )
    return


@app.cell
def _(df_agg, mo, view_toggle):
    # display drill-down selection
    all_labels = sorted(df_agg[df_agg["type"] == view_toggle.value]["label"].unique().tolist())
    label_select = mo.ui.dropdown(
        options=["— select —"] + all_labels,
        value="— select —",
        label=f"Select {view_toggle.value} to drill down:",
    )
    label_select
    return (label_select,)


@app.cell
def _(label_select, make_drill, mo, view_toggle):
    # display drill-down chart
    if label_select.value == "— select —" or label_select.value is None:
        output = mo.md("_👆 Select a topic or member above to see the full cross-category breakdown_")
    else:
        chart = make_drill(label_select.value, view_toggle.value)
        output = mo.as_html(chart) if chart is not None else mo.md("_No data found_")

    output
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
