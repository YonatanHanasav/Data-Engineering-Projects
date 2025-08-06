-- Create projects table
CREATE TABLE IF NOT EXISTS projects (
    project_id UUID PRIMARY KEY,
    store_date DATE NOT NULL,
    initial_date DATE,
    active_date DATE,
    end_date DATE,
    project_name TEXT,
    owner TEXT,
    region TEXT,
    budget NUMERIC
);

-- Create daily project status table that will be used to store the status of the project for each day
CREATE TABLE IF NOT EXISTS daily_project_status (
    project_id UUID,
    project_date DATE,
    status TEXT,
    PRIMARY KEY (project_id, project_date)
);

-- Create etl audit log table that will be used to store the audit log of the etl process
CREATE TABLE IF NOT EXISTS etl_audit_log (
    id SERIAL PRIMARY KEY,
    etl_stage TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count INTEGER,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);