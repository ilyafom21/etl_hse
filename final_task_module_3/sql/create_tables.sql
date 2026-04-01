CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    device VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS etl.session_pages (
    session_id VARCHAR(50) NOT NULL,
    page_order INT NOT NULL,
    page_path TEXT NOT NULL,
    PRIMARY KEY (session_id, page_order),
    FOREIGN KEY (session_id) REFERENCES etl.user_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS etl.session_actions (
    session_id VARCHAR(50) NOT NULL,
    action_order INT NOT NULL,
    action_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (session_id, action_order),
    FOREIGN KEY (session_id) REFERENCES etl.user_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS etl.event_logs (
    event_id VARCHAR(50) PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS etl.support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS etl.ticket_messages (
    ticket_id VARCHAR(50) NOT NULL,
    message_order INT NOT NULL,
    sender VARCHAR(30) NOT NULL,
    message TEXT NOT NULL,
    message_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (ticket_id, message_order),
    FOREIGN KEY (ticket_id) REFERENCES etl.support_tickets(ticket_id) ON DELETE CASCADE
);
