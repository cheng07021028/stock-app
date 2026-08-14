from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
HIDDEN = WIN / "Invoke-GodPickStrictWakeupV191Hidden.vbs"


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def _formal_row(i: int, ret: float) -> dict:
    return {
        "推薦日期": "2026-07-01",
        "更新時間": "2026-08-14 15:00:00",
        "股票代號": f"{1000+i:04d}",
        "正式推薦分區": "正式下週主推薦",
        "紀錄層級": "A｜正式主推薦",
        "推薦模式": "股神正式推薦",
        "推薦後1日%": ret,
        "校正樣本權重": 1.0,
        "是否納入權重校正": "是",
        "推薦總分": 88,
    }


def _near_row(i: int, ret: float) -> dict:
    return {
        "推薦日期": "2026-07-01",
        "更新時間": "2026-08-14 15:00:00",
        "股票代號": f"{5000+i:04d}",
        "正式推薦分區": "盤中雷達追蹤",
        "校正樣本類型": "C｜近門檻對照",
        "紀錄層級": "C｜近門檻對照",
        "推薦模式": "股神校正研究",
        "推薦後1日%": ret,
        # Historical files may still contain H16-era 0.45 + yes. H17 must override it.
        "校正樣本權重": 0.45,
        "是否納入權重校正": "是",
        "推薦總分": 75,
    }


def main() -> None:
    install = INSTALL.read_text(encoding="ascii")
    hidden = HIDDEN.read_text(encoding="ascii")

    check("wscript.exe" in install, "recurring Scheduled Task launches wscript.exe instead of a visible PowerShell console")
    check("//B //Nologo" in install, "wscript runs in batch/no-logo background mode")
    check("New-ScheduledTaskAction -Execute \"powershell.exe\"" not in install,
          "recurring Scheduled Task no longer directly launches powershell.exe")
    check("$StableHiddenLauncherPath" in install and "Copy-Item -LiteralPath $SourceHiddenLauncherPath" in install,
          "installer deploys a stable hidden launcher")
    check('shell.Run(commandLine, 0, True)' in hidden,
          "VBScript explicitly starts PowerShell with window style 0 (hidden)")
    check("-NonInteractive" in hidden, "background dispatcher is non-interactive")
    check("New-TimeSpan -Minutes 10" in install, "strict 10-minute repetition is preserved")
    check("-LogonType Interactive" in install, "same-user interactive principal is preserved for DPAPI PAT decryption")

    from godpick_calibration_sample_service import build_calibration_samples
    from godpick_performance_feedback import build_godpick_performance_profile

    raw = pd.DataFrame([
        {
            "股票代號": "2303", "市場別": "上市", "最新價": 60, "成交額百萬": 500,
            "正式推薦分區": "盤中雷達追蹤", "盤中雷達優先級": "R2-WAIT｜隔日品質未通過",
            "推薦日期": "2026-08-13", "推薦時間": "14:44:15", "推薦總分": 75,
            "Entry進場買點分": 48, "Risk風控安全分": 79, "保守風險報酬比": 1.1,
            "K線落後交易日": 0, "K線資料新鮮度": "最新", "K線最後交易日": "2026-08-13",
            "正式推薦排除原因": "進場可執行分不足",
        }
    ])
    samples = build_calibration_samples(raw, max_near=5, max_missed=5)
    check(not samples.empty and str(samples.iloc[0]["校正樣本類型"]).startswith("C"),
          "R2 near-threshold diagnostic candidate is retained as a C shadow sample")
    check(float(samples.iloc[0]["校正樣本權重"]) == 0.0 and samples.iloc[0]["是否納入權重校正"] == "否",
          "new C samples are shadow-only and cannot immediately alter recommendation weights")

    core = [_formal_row(i, 1.0 if i % 2 == 0 else -0.2) for i in range(20)]
    small_near = [_near_row(i, 3.0) for i in range(10)]
    profile_small = build_godpick_performance_profile(core + small_near)
    diag_small = profile_small.get("near_threshold_shadow_diagnostics", {})
    check(diag_small.get("auto_weight_enabled") is False,
          "10 near-threshold outcomes do not auto-change weights")
    check(float(diag_small.get("effective_weight_per_sample") or 0.0) == 0.0,
          "immature near-threshold cohort keeps effective weight at zero")

    # 30 mature C samples, 80% wins, average return well above core, and no -5% tail loss.
    mature_near = [_near_row(i, 3.0 if i < 24 else -0.5) for i in range(30)]
    profile_mature = build_godpick_performance_profile(core + mature_near)
    diag_mature = profile_mature.get("near_threshold_shadow_diagnostics", {})
    check(diag_mature.get("auto_weight_enabled") is True,
          "only a mature near-threshold cohort with persistent edge may enter learning")
    check(abs(float(diag_mature.get("effective_weight_per_sample") or 0.0) - 0.15) < 1e-9,
          "even proven near-threshold evidence is capped at 15% learning weight")
    check(float(profile_mature.get("data_quality", {}).get("effective_weighted_samples", 0.0)) < 30.0,
          "shadow samples cannot dominate core formal trade evidence")

    print("H17 shadow-first + background wakeup smoke: ALL PASS")


if __name__ == "__main__":
    main()
