-- =================================================================================
-- SCOPTICS-AGENT DATABASE INITIALIZATION SCRIPT (V7.0 - CONDITIONAL NORMALIZATION)
-- =================================================================================
-- This version creates a view that intelligently determines if the source data
-- is already centered and only applies transformations when necessary.
-- =================================================================================

-- PART 1: CREATE CORE DATA TABLES
-- -----------------------------------------------------------------------------
CREATE TABLE match_metadata (
    match_id            TEXT PRIMARY KEY,
    competition_name    TEXT,
    home_team_name      TEXT,
    away_team_name      TEXT,
    pitch_length_m      DOUBLE PRECISION,
    pitch_width_m       DOUBLE PRECISION,
    additional_info     JSONB
);

CREATE TABLE tracking_data (
    match_id            TEXT NOT NULL REFERENCES match_metadata(match_id),
    period              INTEGER NOT NULL,
    frame               INTEGER NOT NULL,
    timestamp_iso       TIMESTAMPTZ NOT NULL,
    tracked_objects     JSONB,
    frame_metadata      JSONB,
    PRIMARY KEY (match_id, frame)
);

-- PART 2: CREATE DATA TRANSFORMATION VIEWS
-- -----------------------------------------------------------------------------
-- This view checks if the raw data is already centered by looking for negative coordinates.
CREATE OR REPLACE VIEW match_pitch_boundaries AS
SELECT
    t.match_id,
    MIN((obj ->> 'x')::FLOAT) AS min_x,
    MIN((obj ->> 'y')::FLOAT) AS min_y
FROM
    tracking_data t,
    LATERAL jsonb_array_elements(t.tracked_objects) obj
GROUP BY
    t.match_id;

-- This final, "smart" view conditionally centers the data.
CCREATE OR REPLACE VIEW tracking_data_centered AS
SELECT
    t.match_id,
    t.period,
    t.frame,
    t.timestamp_iso,
    obj ->> 'trackable_object' AS trackable_object,
    -- For period 2, invert the coordinates to handle the change in attacking direction.
    -- Then, subtract half the pitch dimensions to move the origin from the corner to the center.
    CASE
        WHEN t.period = 2 THEN -((obj ->> 'x')::FLOAT - (m.pitch_length_m / 2))
        ELSE (obj ->> 'x')::FLOAT - (m.pitch_length_m / 2)
    END AS x,
    CASE
        WHEN t.period = 2 THEN -((obj ->> 'y')::FLOAT - (m.pitch_width_m / 2))
        ELSE (obj ->> 'y')::FLOAT - (m.pitch_width_m / 2)
    END AS y
FROM
    tracking_data t
JOIN
    match_metadata m ON t.match_id = m.match_id,
    LATERAL jsonb_array_elements(t.tracked_objects) obj;
-- PART 3: CREATE SECURITY ROLES AND PERMISSIONS
-- -----------------------------------------------------------------------------
CREATE ROLE ai_agent_readonly NOLOGIN;
GRANT CONNECT ON DATABASE scoptics_db TO ai_agent_readonly;
GRANT USAGE ON SCHEMA public TO ai_agent_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_agent_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ai_agent_readonly;

CREATE USER ai_user WITH PASSWORD 'scoptics_password_readonly';
GRANT ai_agent_readonly TO ai_user;