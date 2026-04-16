import os
import json

STATE_FILE = "watchlist_ui_state.json"


def load_watchlist_ui_state():
    default_state = {
        "selected_group": "",
        "stock_keyword": "",
        "selected_candidate_label": "",
        "manual_code": "",
        "manual_name": "",
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


def save_watchlist_ui_state(
    selected_group="",
    stock_keyword="",
    selected_candidate_label="",
    manual_code="",
    manual_name="",
):
    data = {
        "selected_group": selected_group or "",
        "stock_keyword": stock_keyword or "",
        "selected_candidate_label": selected_candidate_label or "",
        "manual_code": manual_code or "",
        "manual_name": manual_name or "",
    }

    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
