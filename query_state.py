import os
import json
from datetime import date, timedelta, datetime

STATE_FILE = "last_query_state.json"


def load_last_query_state():
    today_dt = date.today()
    default_state = {
        "quick_group": "",
        "quick_stock_code": "",
        "home_start": (today_dt - timedelta(days=90)).isoformat(),
        "home_end": today_dt.isoformat(),
    }

    if not os.path.exists(STATE_FILE):
        return default_state

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        for k, v in default_state.items():
            if k not in data:
                data[k] = v

        return data
    except Exception:
        return default_state


def save_last_query_state(quick_group, quick_stock_code, home_start, home_end):
    data = {
        "quick_group": quick_group if quick_group is not None else "",
        "quick_stock_code": quick_stock_code if quick_stock_code is not None else "",
        "home_start": home_start.isoformat() if hasattr(home_start, "isoformat") else str(home_start),
        "home_end": home_end.isoformat() if hasattr(home_end, "isoformat") else str(home_end),
    }

    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def parse_date_safe(date_str, default_value):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return default_value
