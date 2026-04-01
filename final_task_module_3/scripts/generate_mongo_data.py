from pymongo import MongoClient
from datetime import datetime, timedelta
import random

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project"]

user_sessions = db["UserSessions"]
event_logs = db["EventLogs"]
support_tickets = db["SupportTickets"]

user_sessions.delete_many({})
event_logs.delete_many({})
support_tickets.delete_many({})

users = [f"user_{i}" for i in range(1, 21)]
pages = ["/home", "/catalog", "/product/1", "/product/2", "/cart", "/checkout", "/profile"]
actions_list = ["login", "view_page", "view_product", "add_to_cart", "checkout", "logout"]
devices = ["mobile", "desktop", "tablet"]
event_types = ["click", "view", "purchase", "login", "logout"]
issue_types = ["payment", "delivery", "account", "refund"]
statuses = ["open", "closed", "in_progress"]

sessions_data = []
events_data = []
tickets_data = []

base_time = datetime.now() - timedelta(days=30)

for i in range(1, 301):
    start = base_time + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    duration = random.randint(5, 120)
    end = start + timedelta(minutes=duration)

    session_id = f"sess_{i:04d}"
    user_id = random.choice(users)

    visited_pages = random.sample(pages, random.randint(2, 5))
    session_actions = random.sample(actions_list, random.randint(2, 5))

    sessions_data.append({
        "session_id": session_id,
        "user_id": user_id,
        "start_time": start,
        "end_time": end,
        "pages_visited": visited_pages,
        "device": random.choice(devices),
        "actions": session_actions
    })

for i in range(1, 501):
    ts = base_time + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    events_data.append({
        "event_id": f"evt_{i:04d}",
        "timestamp": ts,
        "event_type": random.choice(event_types),
        "details": random.choice(pages)
    })

for i in range(1, 151):
    created = base_time + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    updated = created + timedelta(hours=random.randint(1, 72))

    tickets_data.append({
        "ticket_id": f"ticket_{i:04d}",
        "user_id": random.choice(users),
        "status": random.choice(statuses),
        "issue_type": random.choice(issue_types),
        "messages": [
            {
                "sender": "user",
                "message": "Помогите решить проблему.",
                "timestamp": created
            },
            {
                "sender": "support",
                "message": "Мы уже занимаемся вашим вопросом.",
                "timestamp": updated
            }
        ],
        "created_at": created,
        "updated_at": updated
    })

if sessions_data:
    user_sessions.insert_many(sessions_data)

if events_data:
    event_logs.insert_many(events_data)

if tickets_data:
    support_tickets.insert_many(tickets_data)

print("Данные успешно загружены в MongoDB")
print(f"UserSessions: {user_sessions.count_documents({})}")
print(f"EventLogs: {event_logs.count_documents({})}")
print(f"SupportTickets: {support_tickets.count_documents({})}")
