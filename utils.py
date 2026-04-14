WATCHLIST_FILE = Path("watchlist.json")

DEFAULT_WATCHLIST = {
    "半導體": ["2330", "2454", "3711"],
    "AI": ["2317", "2382"],
    "ETF": ["0050", "0056"],
    "金融": ["2881", "2882"],
    "我的觀察名單": []
}


def load_watchlist():
    if not WATCHLIST_FILE.exists():
        save_watchlist(DEFAULT_WATCHLIST)
        return DEFAULT_WATCHLIST.copy()

    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 舊版 list 自動轉新格式
        if isinstance(data, list):
            return {
                "我的觀察名單": [str(x).strip() for x in data if str(x).strip()]
            }

        # 新版 dict 格式
        if isinstance(data, dict):
            clean_data = {}
            for group_name, codes in data.items():
                clean_group = [str(x).strip() for x in codes if str(x).strip()]
                clean_data[str(group_name).strip()] = list(dict.fromkeys(clean_group))
            return clean_data

        return DEFAULT_WATCHLIST.copy()

    except Exception:
        return DEFAULT_WATCHLIST.copy()


def save_watchlist(watchlist_dict):
    clean_dict = {}

    for group_name, codes in watchlist_dict.items():
        group_name = str(group_name).strip()
        if not group_name:
            continue

        clean_codes = [str(x).strip() for x in codes if str(x).strip()]
        clean_dict[group_name] = list(dict.fromkeys(clean_codes))

    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(clean_dict, f, ensure_ascii=False, indent=2)


def flatten_watchlist_groups(watchlist_dict):
    """
    把分組自選股攤平成單一代號清單
    """
    all_codes = []
    for _, codes in watchlist_dict.items():
        all_codes.extend(codes)
    return list(dict.fromkeys(all_codes))