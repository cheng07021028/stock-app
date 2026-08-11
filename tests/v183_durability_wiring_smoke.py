from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
checks = {
    'page0_market': (ROOT/'pages/0_大盤走勢.py', ['persist_json_async', 'MARKET_TREND_RECORDS_FILE']),
    'nextday': (ROOT/'market_nextday_forecast_engine.py', ['persist_json_async', 'nextday forecast history']),
    'official': (ROOT/'official_factor_service.py', ['official factor cache', 'institutional history']),
    'stock_master': (ROOT/'stock_master_service.py', ['persist_json_async', 'stock_master_cache.json']),
    'dashboard_settings': (ROOT/'pages/1_儀表板.py', ['persist_json_async', 'dashboard table settings']),
    'hk_settings': (ROOT/'pages/3_歷史K線分析.py', ['persist_json_async', 'history K chart settings']),
    'page17': (ROOT/'pages/17_系統健康檢查.py', ['queue_existing_critical_for_migration', 'audit_core_durability']),
    'page7_super_ai': (ROOT/'pages/7_股神推薦.py', ['save_super_ai_run', 'SuperAI']),
    'durable_registry': (ROOT/'godpick_durability_service.py', [
        'godpick_user_settings.json','godpick_record_ui_config.json','godpick_management_ui_config.json',
        'dashboard_table_settings.json','hk_chart_settings.json','official_factor_schedule_settings.json',
        'skip_generic_migration'
    ]),
}
for name,(path,needles) in checks.items():
    text=path.read_text(encoding='utf-8-sig')
    missing=[x for x in needles if x not in text]
    assert not missing, f'{name} missing {missing}'
print('PASS V183 durability wiring｜authority data + AI experience + persistent settings wired')
