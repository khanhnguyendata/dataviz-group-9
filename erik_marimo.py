import marimo

__generated_with = "0.23.0"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Trips explorer
    """)
    return


@app.cell(hide_code=True)
def _():
    import os
    import sqlite3
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import pandas as pd

    from db_utils import get_trip_export_query, load_config

    return Path, alt, mo, os, pd, sqlite3

@app.cell(hide_code=True)
def _(Path, os, pd):
    DATA_DIR = Path(
        os.getenv("DVDS_TRIP_DATA_DIR", "data/trips")
    )

    def load_places() -> pd.DataFrame:
        path = DATA_DIR / "places.csv"
        if path.exists():
            return pd.read_csv(path)
        return pd.DataFrame(columns=["place_id", "zone"])

    def load_events() -> pd.DataFrame:
        path = DATA_DIR / "trip_entries.csv"
        if path.exists():
            return pd.read_csv(path).rename(
                columns={"person_name": "person"}
            )
        return pd.DataFrame()

    return DATA_DIR, load_events, load_places

@app.cell(hide_code=True)
def _(DATA_DIR, load_events, load_places):
    events = load_events()
    places = load_places()

    if "entry_id" not in events.columns:
        events.insert(0, "entry_id", range(1, len(events) + 1))

    events["event_time"] = pd.to_datetime(
        events["event_time"].astype(str).str.replace(r"^0040", "2040", regex=True),
        errors="coerce",
    )
    events["lat"] = pd.to_numeric(events["lat"], errors="coerce")
    events["lon"] = pd.to_numeric(events["lon"], errors="coerce")

    if "source_presence" not in events.columns:
        events["source_presence"] = (
            events["database"] if "database" in events.columns else "A"
        )
    events["source_presence"] = (
        events["source_presence"].replace({"Both": "BOTH"}).astype(str).str.upper()
    )

    if "zone" not in events.columns:
        if (
            not places.empty
            and "place_id" in places.columns
            and "zone" in places.columns
        ):
            places_lookup = (
                places[["place_id", "zone"]]
                .dropna(subset=["place_id"])
                .drop_duplicates(subset=["place_id"])
                .copy()
            )
            events = events.merge(places_lookup, on="place_id", how="left")
        else:
            events["zone"] = pd.NA

    events = events.dropna(
        subset=["source_presence", "person", "event_time", "lat", "lon"]
    ).copy()
    events["zone"] = events.get(
        "zone", pd.Series(index=events.index, dtype="object")
    ).fillna("Unknown zone")
    events["shared_in_both"] = events["source_presence"] == "BOTH"
    events["event_day"] = events["event_time"].dt.strftime("%Y-%m-%d")
    events["event_key"] = (
        events["source_presence"].astype(str)
        + "|"
        + events["trip_id"].astype(str)
        + "|"
        + events["person"].astype(str)
        + "|"
        + events["place_id"].astype(str)
        + "|"
        + events["event_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    events = events.sort_values(
        ["event_time", "source_presence", "person", "trip_id"], kind="stable"
    ).reset_index(drop=True)
    return (events,)


@app.cell
def _(events):
    lon_min = float(events["lon"].min())
    lon_max = float(events["lon"].max())
    lat_min = float(events["lat"].min())
    lat_max = float(events["lat"].max())
    global_lon_domain = [lon_min, lon_max] if lon_min != lon_max else [lon_min - 0.5, lon_max + 0.5]
    global_lat_domain = [lat_min, lat_max] if lat_min != lat_max else [lat_min - 0.5, lat_max + 0.5]
    return global_lat_domain, global_lon_domain


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Pick a person and date range, brush the timeline or drag a rectangle on the map to cross-highlight entries.
    Click a single point to focus one event (double-click to clear).
    """)
    return


@app.cell(hide_code=True)
def _(events, mo):
    database_toggle = mo.ui.radio(
        options=["Journalist", "Both datasets"],
        value="Both datasets",
        label="Source",
    )

    color_toggle = mo.ui.radio(
        options=["person", "zone"],
        value="person",
        label="Color by",
    )


    min_day = events["event_time"].min().normalize()
    max_day = events["event_time"].max().normalize()

    start_date = mo.ui.date(value=min_day.date().isoformat(), label="Start date")
    end_date = mo.ui.date(value=max_day.date().isoformat(), label="End date")

    days = (
        sorted(events["event_day"].dropna().unique().tolist())
        if "event_day" in events.columns
        else []
    )
    day_toggle = mo.ui.dropdown(
        options=["ALL", *days], value="ALL", label="Day zoom"
    )


    controls = mo.hstack(
        [
            database_toggle,
            color_toggle,
            # person_toggle,
            start_date,
            end_date,
            day_toggle,
        ]
    )

    controls
    return color_toggle, database_toggle, day_toggle, end_date, start_date


@app.cell
def _(database_toggle, day_toggle, end_date, events, pd, start_date):
    filtered_events = events

    if database_toggle.value != "Both datasets":
        value_map = {"A": "A", "Journalist": "B"}
        selected = value_map.get(database_toggle.value, "BOTH")
        filtered_events = filtered_events[filtered_events["source_presence"] == selected]

    start_day = pd.Timestamp(start_date.value).normalize()
    end_day = pd.Timestamp(end_date.value).normalize()
    if end_day < start_day:
        start_day, end_day = end_day, start_day

    start_ts = start_day
    end_ts = end_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    filtered_events = filtered_events[
        (filtered_events["event_time"] >= start_ts) & (filtered_events["event_time"] <= end_ts)
    ].copy()

    if day_toggle.value != "ALL":
        filtered_events = filtered_events[filtered_events["event_day"] == day_toggle.value].copy()
        day_ts = pd.Timestamp(day_toggle.value)
        timeline_start = day_ts
        timeline_end = day_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        timeline_start = start_ts
        timeline_end = end_ts

    filtered_events["display_database"] = filtered_events["source_presence"]
    filtered_events["base_opacity"] = 0.5
    return filtered_events, timeline_end, timeline_start


@app.cell
def _(alt, color_toggle, events, filtered_events):
    UNKNOWN_TRIP = "unknown trip"
    UNKNOWN_PLACE = "unknown place"
    TIMELINE_HEIGHT = 460

    df = filtered_events.copy()

    # --- clean data ---
    df["trip_id"] = df["trip_id"].fillna(UNKNOWN_TRIP)
    df["place_name"] = df["place_name"].fillna(UNKNOWN_PLACE)
    df["zone"] = df["zone"].fillna("Unknown zone")

    all_people = sorted(events["person"].dropna().unique().tolist())

    # --- color base ---
    color_field = "person" if color_toggle.value == "person" else "zone"
    color_title = "Person" if color_toggle.value == "person" else "Zone"


    map_brush = alt.selection_interval(encodings=["x", "y"])
    timeline_brush = alt.selection_interval(encodings=["x"])
    event_pick = alt.selection_point(fields=["event_key"], on="click", clear="dblclick")


    # --- SELECTION (clickable legend/person) ---
    selection = alt.selection_point(
        fields=["person"],
        bind="legend",        
    
        empty="all"          
    )

    selection_zone = alt.selection_point(
        fields=["zone"],
        bind="legend",         
        empty="all"          
    )



    # --- CONDITIONAL COLOR ---
    base_color = alt.Color(
        f"{color_field}:N",
        title=color_title,
        scale=alt.Scale(scheme="tableau10"),
        legend=alt.Legend(orient="top", direction="vertical"),
    )



    color = alt.condition(
    
    
        selection
        ,
        base_color,
        alt.value("gray")   
    )

    shape = alt.Shape(
        "display_database:N",
        title="Source",
        scale=alt.Scale(
            domain=["A", "B", "BOTH"],
            range=["circle", "diamond", "circle"]
        ),
        legend=alt.Legend(orient="top", direction="vertical"),
    )

    strokeDash = alt.StrokeDash(
        "display_database:N",
        title="Source",
        scale=alt.Scale(
            domain=["A", "B"],
            range=[[1, 0], [6, 2]]
        ),
        legend=alt.Legend(orient="top", direction="vertical"),
    )

    return (
        TIMELINE_HEIGHT,
        all_people,
        color,
        df,
        event_pick,
        map_brush,
        selection,
        shape,
        strokeDash,
        timeline_brush,
    )


@app.cell
def _(
    alt,
    color,
    df,
    event_pick,
    global_lat_domain,
    global_lon_domain,
    map_brush,
    selection,
    shape,
    strokeDash,
    timeline_brush,
):
    # =====================
    # MAP
    # =====================
    map_base = alt.Chart(df).encode(
        x=alt.X(
            "lon:Q", title="Longitude", scale=alt.Scale(domain=global_lon_domain)
        ),
        y=alt.Y(
            "lat:Q", title="Latitude", scale=alt.Scale(domain=global_lat_domain)
        ),
        tooltip=["person", "zone", "trip_id", "place_name", "event_time"],
    )

    map_background = map_base.mark_line(strokeWidth=1.5).encode(
        detail=["source_presence:N", "person:N", "event_day:N"],
        order="event_time:T",
        color=color,
        strokeDash=strokeDash,
        opacity=alt.value(0.6),
    ) + map_base.mark_point(size=30, filled=True).encode(
        color=color, shape=shape
        # , opacity=alt.value(0.6)
    )

    map_highlight = (
        map_base
        .transform_filter(event_pick)
        .transform_filter(timeline_brush)
        .transform_filter(map_brush)
        .mark_point(size=100, filled=True, stroke="black", strokeWidth=0.5)
        .encode(color=color, shape=shape)
    )


    trip_map = (
        (map_background + map_highlight)
        .add_params(map_brush, event_pick, selection)
        .properties(
            width="container",
            height=500,
            title="Trip map",
        )
    )
    return (trip_map,)


@app.cell
def _(
    TIMELINE_HEIGHT,
    all_people,
    alt,
    color,
    df,
    event_pick,
    map_brush,
    selection,
    shape,
    timeline_brush,
    timeline_end,
    timeline_start,
):
    # =====================
    # TIMELINE (FULL WIDTH + ALL PEOPLE)
    # =====================
    timeline_base = alt.Chart(df).encode(
        x=alt.X(
            "event_time:T",
            title="Entry time",
            scale=alt.Scale(domain=[timeline_start, timeline_end]),
        ),
        y=alt.Y(
            "person:N",
            title="Person",
            sort=all_people,  # ensures all people always shown
        ),
        tooltip=["person", "zone", "trip_id", "place_name", "event_time"],
    )

    timeline_background = timeline_base.mark_point(size=30, filled=True).encode(
        color=color, shape=shape
    )

    timeline_highlight = (
        timeline_base.transform_filter(event_pick)
        .transform_filter(timeline_brush)
        .transform_filter(map_brush)
        .mark_point(filled=True, size=100, stroke="black", strokeWidth=0.5)
        .encode(color=color, shape=shape)
    )

    timeline = (
        (timeline_background + timeline_highlight)
        .add_params(timeline_brush, event_pick, selection)
        .properties(
            width="container",  # FULL WIDTH
            height=TIMELINE_HEIGHT,
            title="Entry timeline",
        )
    )
    return (timeline,)


@app.cell
def _(alt, timeline, trip_map):
    # =====================
    # FINAL DASHBOARD
    # =====================
    dashboard = (
        alt.vconcat(timeline, trip_map)
        .resolve_scale(color="shared", shape="shared")
        .resolve_legend(color="shared", shape="shared")
        .properties(
            autosize="fit-x",
        
        )
    )

    dashboard
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
