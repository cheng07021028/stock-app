# -*- coding: utf-8 -*-
"""V191-H151.2: Excel-compliant frozen view repair for multi-sheet reports.

Worksheet.freeze_panes is *not* idempotent in openpyxl: assigning D2 and
then D8, or D2 and then A2, retains selections from the old view.  The
result is writable and readable in Python, but Excel may repair the OOXML.
This module corrects the final workbook's view metadata, not the data.
"""
from __future__ import annotations

from typing import Any

VERSION = "V191-H151.2-EXCEL-VIEW-INTEGRITY"


def normalize_frozen_sheet_views(workbook: Any) -> dict[str, dict[str, object]]:
    """Rebuild sheet-view selections once, preserving the final freeze cell.

    Do not change cells, workbook tabs/order, formulae, filters or trading
    decisions.  Each frozen sheet must have one coherent set of selections:
    one for a single-axis freeze or three for a dual-axis freeze.
    """
    from openpyxl.worksheet.views import Selection

    result: dict[str, dict[str, object]] = {}
    for sheet in workbook.worksheets:
        view = sheet.sheet_view
        pane = view.pane
        coordinate = str(pane.topLeftCell or "") if pane is not None else ""
        old_count = len(view.selection)
        if coordinate and coordinate != "A1":
            # Restore the intended final pane; reset stale prior selections
            # BEFORE calling the openpyxl freeze setter.
            view.selection = [Selection()]
            sheet.freeze_panes = coordinate
        else:
            view.pane = None
            view.selection = [Selection()]
        pane = view.pane
        if pane is not None:
            if pane.xSplit and pane.ySplit:
                wanted = ("topRight", "bottomLeft", "bottomRight")
            elif pane.xSplit:
                wanted = ("topRight",)
            else:
                wanted = ("bottomLeft",)
            actual = tuple(sel.pane for sel in view.selection)
            if actual != wanted or pane.activePane != wanted[-1]:
                raise ValueError(
                    f"Excel frozen-view inconsistent on {sheet.title}: "
                    f"expected {wanted}, got {actual}, active={pane.activePane}"
                )
        else:
            if len(view.selection) != 1:
                raise ValueError(f"Unfrozen sheet {sheet.title} has duplicate view selections")
        result[sheet.title] = {"freeze": coordinate or None,
                               "previous_selections": old_count,
                               "normalised_selections": len(view.selection)}
    return result


__all__ = ["VERSION", "normalize_frozen_sheet_views"]
