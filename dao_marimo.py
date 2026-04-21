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
):
    ### THIS MAPPING IS SUBJECT TO CHANGE!!!
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

    return gov_disc, jnl_disc

@app.cell
def _(gov_disc, jnl_disc, pd):
    df = pd.concat([jnl_disc, gov_disc], ignore_index=True)[["source", "industry_category", "disc_topic_id", "discussion_id", "disc_person_reason", "person_name", "role", "disc_person_sentiment"]]
    df.head()
    return (df,)


@app.cell
def _(df, pd):
    topic_agg = (df
        .groupby(["source", "industry_category", "disc_topic_id"])
        .agg(avg_sentiment=('disc_person_sentiment', 'mean'), disc_cnt=('discussion_id', 'nunique'))
        .reset_index()
                )

    topic_agg[["type"]] = "Topic"
    topic_agg.rename(columns={"disc_topic_id": "label", "disc_person_sentiment": "sentiment"}, inplace=True)

    member_agg = (df
        .groupby(["source", "industry_category", "person_name"])
        .agg(avg_sentiment=('disc_person_sentiment', 'mean'), disc_cnt=('discussion_id', 'nunique'))
        .reset_index()
                )
    member_agg[["type"]] = "Member"
    member_agg.rename(columns={"person_name": "label", "disc_person_sentiment": "sentiment"}, inplace=True)

    df_agg = pd.concat([topic_agg, member_agg], ignore_index=True)
    df_agg
    return (df_agg,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Visualization
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(alt, df_agg, mo):

    BG_SINGLE     = "#E8EDF7"   # cool blue  — only one source
    BG_OVERLAPPING = "#FFF0E0"  # warm amber — both sources present
    #  ── 3. Toggle selection ───────────────────────────────────────────────────────
    toggle = alt.selection_point(
        name="toggle",
        fields=["type"],
        bind=alt.binding_radio(options=["Topic", "Member"], name="View by: "),
        value="Topic",
    )

    color = alt.Color(
        "source:N",
        scale=alt.Scale(domain=["board", "journalist"], range=["#000080", "#CD5C5C"]),
        legend=alt.Legend(title="Source", clipHeight=30)
    )
    # ── Cell 1: chart function per category ──────────────────────────────────────
    def make_lane3(category, df_filtered):
        """One swim lane = one chart built from pre-filtered data."""

        # ── Pivot wide for horizontal connecting lines ────────────────────────────
        df_wide = (
            df_filtered
            .pivot_table(
                index=["type", "label"],
                columns="source",
                values="avg_sentiment"
            )
            .reset_index()
        )
        df_wide.columns.name = None

        # ── Category avg per source for vertical lines ────────────────────────────
        df_avg = (
            df_filtered
            .groupby("source")["avg_sentiment"]
            .mean()
            .reset_index()
            .rename(columns={"avg_sentiment": "cat_avg"})
        )
      # ── Background strips ─────────────────────────────────────────────────────
        bg = (
            alt.Chart(df_wide)
            .mark_rect(opacity=0.4)
            .encode(
                y=alt.Y("label:N", title=None, axis=alt.Axis(labelFontSize=12)),
                color=alt.condition(
                    "datum.has_both",
                    alt.value(BG_OVERLAPPING),
                    alt.value(BG_SINGLE),
                ),
            )
            .transform_filter(toggle)
        )

        # ── Dots ──────────────────────────────────────────────────────────────────
        dots = (
            alt.Chart(df_filtered)
            .mark_circle(opacity=0.8, stroke='black', strokeWidth=1,
                         strokeOpacity=0.4, size=80)
            .encode(
                alt.X('avg_sentiment:Q')
                    .title("Average sentiment" if category == "tourism" else "")
                    .scale(domain=[-1, 1]),
                alt.Y('label:N')
                    .title(None)
                    .axis(alt.Axis(labelFontSize=12)),
                alt.Size('disc_cnt:Q')
                    .title('# Discussions')
                    .legend(clipHeight=30, format='s'),
                color,
                tooltip=[
                    alt.Tooltip("source:N",        title="Data source:"),
                    alt.Tooltip("type_label:N",     title="Granularity:"),
                    alt.Tooltip("avg_sentiment:Q",  title="Avg sentiment:", format=".2f"),
                    alt.Tooltip("disc_cnt:Q",       title="# discussions:", format='~s'),
                ],
            )
            .transform_calculate(type_label="datum.type + ' ' + datum.label")
            .transform_filter(toggle)
            .add_params(toggle)
        )

        # ── Horizontal lines: connect journalist ↔ board per label ────────────────
        horizontal_lines = (
            alt.Chart(df_wide)
            .mark_rule(strokeWidth=1.5, opacity=0.5)
            .encode(
                x=alt.X("journalist:Q", scale=alt.Scale(domain=[-1, 1])),
                x2="board:Q",
                y=alt.Y("label:N"),
                tooltip=[
                    "label:N",
                    alt.Tooltip("journalist:Q", title="Journalist:", format=".3f"),
                    alt.Tooltip("board:Q",      title="Board:",      format=".3f"),
                ],
            )
            .transform_filter(toggle)
            .add_params(toggle)
        )

        # ── Vertical lines: avg sentiment per source for this category ────────────
        vertical_lines = (
            alt.Chart(df_avg)
            .mark_rule(strokeWidth=3, opacity=0.9, strokeDash=[4, 2])
            .encode(
                x=alt.X("cat_avg:Q", scale=alt.Scale(domain=[-1, 1])),
                color=color, 
                # color=alt.Color(
                #     "source:N",
                #     scale=alt.Scale(
                #         domain=["journalist", "board"],
                #         range=["#000080", "#CD5C5C"]
                #     ),
                # ),
                tooltip=[
                    alt.Tooltip("cat_avg:Q", title="Category avg:", format=".2f"),
                    "source:N",
                ],
            )
            # .transform_filter(toggle)
            # .add_params(toggle)
        )

        # ── Avg value text labels above vertical lines ────────────────────────────
        avg_text = (
            alt.Chart(df_avg)
            .mark_text(dy=-8, fontSize=11, fontWeight="bold")
            .mark_text(
                dy=-8,       # vertical offset (up)
                dx=12,       # ← horizontal offset (right)
                fontSize=11,
                fontWeight="bold",
        )
            .encode(
                x=alt.X("cat_avg:Q", scale=alt.Scale(domain=[-1, 1])),
                text=alt.Text("cat_avg:Q", format=".2f"),
                color=color,
            )
            # .transform_filter(toggle)
            # .add_params(toggle)
        )

        return (
            alt.layer(horizontal_lines, dots, vertical_lines, avg_text)
            .properties(
                width=520,
                height=alt.Step(32),
                title=alt.Title(
                    text=category.upper(),
                    anchor="start",
                    fontSize=14,
                    fontWeight="bold",
                )
            )
        )


    # ── Cell 2: reactive on toggle ────────────────────────────────────────────────
    def make_chart3(view_type):
        df_filtered = df_agg[df_agg["type"] == view_type].copy()

        lanes = [
            make_lane3(cat, df_filtered[df_filtered["industry_category"] == cat])
            for cat in ["fishing", "mixed", "tourism"]
            if cat in df_filtered["industry_category"].values
        ]

        return (
            alt.vconcat(*lanes)
            .resolve_scale(color="shared", size="shared")
            .configure_view(stroke="lightgray")
            .configure_axisY(domain=False, ticks=False, offset=10)
            .configure_axis(labelFontSize=11, titleFontSize=12)
        )


    # ── Cell 3: display ───────────────────────────────────────────────────────────
    mo.ui.altair_chart(make_chart3(toggle.value))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
