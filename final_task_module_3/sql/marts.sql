CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.user_activity AS
SELECT
    us.user_id,
    COUNT(DISTINCT us.session_id) AS sessions_count,
    AVG(EXTRACT(EPOCH FROM (us.end_time - us.start_time))) AS avg_session_duration_sec,
    COUNT(DISTINCT sp.session_id || '_' || sp.page_order) AS total_pages_visited,
    COUNT(DISTINCT sa.session_id || '_' || sa.action_order) AS total_actions
FROM etl.user_sessions us
LEFT JOIN etl.session_pages sp
    ON us.session_id = sp.session_id
LEFT JOIN etl.session_actions sa
    ON us.session_id = sa.session_id
GROUP BY us.user_id;

CREATE TABLE IF NOT EXISTS mart.support_stats AS
SELECT
    status,
    issue_type,
    COUNT(*) AS tickets_count,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0) AS avg_resolution_hours
FROM etl.support_tickets
GROUP BY status, issue_type;
