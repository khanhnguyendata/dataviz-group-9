-- Schema for journalist_csv dataset
-- Generated from VAST Challenge 2025 MC2
-- All junction table columns match entity table PKs for direct JOINs.

-- ══ Entity tables ══════════════════════════════════════════

CREATE TABLE meetings (
    meeting_id  TEXT PRIMARY KEY,
    date        TEXT,
    label       TEXT
);

CREATE TABLE people (
    people_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    role        TEXT
);

CREATE TABLE organizations (
    organization_id TEXT PRIMARY KEY,
    name            TEXT
);

CREATE TABLE topics (
    topic_id    TEXT PRIMARY KEY,
    short_topic TEXT,
    long_topic  TEXT
);

CREATE TABLE discussions (
    discussion_id TEXT PRIMARY KEY,
    short_title   TEXT,
    long_title    TEXT
);

CREATE TABLE plans (
    plan_id     TEXT PRIMARY KEY,
    short_title TEXT,
    long_title  TEXT,
    plan_type   TEXT
);

CREATE TABLE places (
    place_id    TEXT PRIMARY KEY,
    name        TEXT,
    lat         REAL,
    lon         REAL,
    zone        TEXT,
    zone_detail TEXT
);

CREATE TABLE trips (
    trip_id     TEXT PRIMARY KEY,
    date        TEXT,
    start_time  TEXT,
    end_time    TEXT
);

-- ══ Junction tables ════════════════════════════════════════

-- People participating in discussions (with sentiment)
CREATE TABLE discussion_people_participations (
    discussion_id TEXT NOT NULL,
    people_id     TEXT NOT NULL,
    sentiment     REAL,
    reason        TEXT,
    industry      TEXT,
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id),
    FOREIGN KEY (people_id) REFERENCES people(people_id)
);

-- Organizations participating in discussions (with sentiment)
CREATE TABLE discussion_org_participations (
    discussion_id   TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    sentiment       REAL,
    reason          TEXT,
    industry        TEXT,
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id),
    FOREIGN KEY (organization_id) REFERENCES organizations(organization_id)
);

-- People participating in plans (with sentiment)
CREATE TABLE plan_people_participations (
    plan_id     TEXT NOT NULL,
    people_id   TEXT NOT NULL,
    sentiment   REAL,
    reason      TEXT,
    industry    TEXT,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id),
    FOREIGN KEY (people_id) REFERENCES people(people_id)
);

-- Organizations participating in plans (with sentiment)
CREATE TABLE plan_org_participations (
    plan_id         TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    sentiment       REAL,
    reason          TEXT,
    industry        TEXT,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id),
    FOREIGN KEY (organization_id) REFERENCES organizations(organization_id)
);

-- Meeting contains discussions
CREATE TABLE meeting_discussions (
    meeting_id    TEXT NOT NULL,
    discussion_id TEXT NOT NULL,
    FOREIGN KEY (meeting_id) REFERENCES meetings(meeting_id),
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id)
);

-- Meeting contains plans
CREATE TABLE meeting_plans (
    meeting_id TEXT NOT NULL,
    plan_id    TEXT NOT NULL,
    FOREIGN KEY (meeting_id) REFERENCES meetings(meeting_id),
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);

-- Discussion is about a topic
CREATE TABLE discussion_topics (
    discussion_id TEXT NOT NULL,
    topic_id      TEXT NOT NULL,
    status        TEXT,
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id),
    FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
);

-- Discussion is about a plan
CREATE TABLE discussion_plans (
    discussion_id TEXT NOT NULL,
    plan_id       TEXT NOT NULL,
    status        TEXT,
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id),
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);

-- Plan relates to topic
CREATE TABLE plan_topics (
    plan_id     TEXT NOT NULL,
    topic_id    TEXT NOT NULL,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id),
    FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
);

-- Plan involves travel to a place
CREATE TABLE travel_links (
    plan_id     TEXT NOT NULL,
    place_id    TEXT NOT NULL,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id),
    FOREIGN KEY (place_id) REFERENCES places(place_id)
);

-- Discussion refers to a place
CREATE TABLE refers_to (
    discussion_id TEXT NOT NULL,
    place_id      TEXT NOT NULL,
    FOREIGN KEY (discussion_id) REFERENCES discussions(discussion_id),
    FOREIGN KEY (place_id) REFERENCES places(place_id)
);

-- Trip visits a person
CREATE TABLE trip_people (
    trip_id   TEXT NOT NULL,
    people_id TEXT NOT NULL,
    time      TEXT,
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (people_id) REFERENCES people(people_id)
);

-- Trip visits a place
CREATE TABLE trip_places (
    trip_id  TEXT NOT NULL,
    place_id TEXT NOT NULL,
    time     TEXT,
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (place_id) REFERENCES places(place_id)
);
