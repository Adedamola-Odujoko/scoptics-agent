-- =============================================================================
-- SCOPTICS-AGENT DATABASE INITIALIZATION SCRIPT (V3.0 - CLEAN START)
-- =============================================================================
-- This script assumes it is running on a fresh, clean database.
-- It only contains the necessary CREATE and GRANT commands.
-- =============================================================================

-- PART 1: CREATE SCHEMA
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
    timestamp_iso       TIMESTAMPTZ NOT NULL, -- Corrected typo from previous version
    tracked_objects     JSONB,
    frame_metadata      JSONB,
    PRIMARY KEY (match_id, frame)
);


-- PART 2: CREATE SECURITY ROLES AND PERMISSIONS
-- -----------------------------------------------------------------------------
-- Create the role and user from scratch.
-- -----------------------------------------------------------------------------
CREATE ROLE ai_agent_readonly NOLOGIN;
GRANT CONNECT ON DATABASE scoptics_db TO ai_agent_readonly;
GRANT USAGE ON SCHEMA public TO ai_agent_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_agent_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ai_agent_readonly;

CREATE USER ai_user WITH PASSWORD 'scoptics_password_readonly';
GRANT ai_agent_readonly TO ai_user; -- Correctly grant the role TO the user