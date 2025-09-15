-- =============================================================================
-- SCOPTICS-AGENT DATABASE INITIALIZATION SCRIPT
-- =============================================================================
-- This script defines the core tables for storing tracking data and events,
-- and sets up the necessary security roles for the application.
-- =============================================================================


-- TBL.01: TRACKING DATA TABLE
-- -----------------------------------------------------------------------------
-- This table stores the raw, frame-by-frame positional data for every player
-- and the ball for an entire match. It is the foundational source of truth.
-- `IF NOT EXISTS` prevents an error if we run the script multiple times.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tracking (
    match_id            TEXT NOT NULL,
    frame               INTEGER NOT NULL,
    -- `TIMESTAMPTZ` (Timestamp with Time Zone) is the best practice for storing time.
    -- It is unambiguous and avoids all timezone-related bugs.
    timestamp_iso       TIMESTAMPTZ NOT NULL,
    team_id             TEXT,
    player_id           TEXT,
    -- `DOUBLE PRECISION` is the standard SQL type for high-precision floating-point numbers.
    x                   DOUBLE PRECISION,
    y                   DOUBLE PRECISION,
    z                   DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    orientation         DOUBLE PRECISION
);


-- TBL.02: EVENTS TABLE
-- -----------------------------------------------------------------------------
-- This table stores the discrete, pre-computed tactical events that are
-- extracted from the raw tracking data. This is the "fast path" table for
-- common queries.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    -- `PRIMARY KEY` ensures every event has a unique ID.
    event_id            TEXT PRIMARY KEY,
    match_id            TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ NOT NULL,
    start_frame         INTEGER NOT NULL,
    end_frame           INTEGER NOT NULL,
    team_id             TEXT,
    -- `JSONB` is the binary JSON format. It is much more efficient to store and
    -- query than plain text or the regular JSON type. Perfect for lists or objects.
    players_involved    JSONB,
    metadata_json       JSONB
);


-- SEC.01: SECURITY ROLES AND PERMISSIONS
-- -----------------------------------------------------------------------------
-- This is a critical security measure. We create a special, restricted role
-- that our AI agent will use. This role can ONLY read data, preventing any
-- possibility of accidental or malicious data modification.
-- -----------------------------------------------------------------------------

-- First, we create the role itself. It's like a permission template. `NOLOGIN` means
-- you can't log in directly with this role name.
CREATE ROLE ai_agent_readonly NOLOGIN;

-- Grant the ability for this role to connect to our specific database.
GRANT CONNECT ON DATABASE scoptics_db TO ai_agent_readonly;

-- Grant the ability for this role to even see the 'public' schema where tables live.
GRANT USAGE ON SCHEMA public TO ai_agent_readonly;

-- The most important rule: Grant ONLY `SELECT` (read) permission on all current tables.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_agent_readonly;

-- CRITICAL: This ensures that any tables created in the FUTURE will also automatically
-- have `SELECT` permission granted to this role.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ai_agent_readonly;

-- Now, we create the actual user account that our application will use for queries.
-- `DO UPDATE` handles the case where the user already exists.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'ai_user') THEN
        CREATE USER ai_user WITH PASSWORD 'scoptics_password_readonly';
    END IF;
END
$$;

-- Finally, we assign the read-only permission template (`ai_agent_readonly`) to our user.
GRANT ai_agent_readonly TO ai_user;