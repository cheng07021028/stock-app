# -*- coding: utf-8 -*-
"""H250-R3: native table boundary compatibility, before page/global wrappers.

Only the GodPick private routing flag is consumed. Do not catch renderer
exceptions, modify data, suppress other kwargs, render widgets, or change UI.
Full/light manager wrappers keep their own bypass semantics.
"""
from functools import wraps
from threading import RLock
import logging

VERSION = 'V191-H250-R3'
_MARK = '_godpick_h250r3_native_guard'
_LOCK = RLock()


def _guard(native):
    if not callable(native):
        raise TypeError('Streamlit table renderer not callable')
    if getattr(native, _MARK, False):
        return native

    @wraps(native)
    def safe(*args, **kwargs):
        kwargs.pop('_godpick_bypass', None)
        return native(*args, **kwargs)

    setattr(safe, _MARK, True)
    return safe


def install(st=None):
    """Install before any GodPick UI imports; idempotent on app reruns.

    Existing full/light wrapper flags are honored on reruns: only their saved
    originals are guarded, never the managed wrapper itself (which must see
    the bypass flag). Reboot is required to clear older captured callables.
    """
    if st is None:
        import streamlit as st
    with _LOCK:
        full = bool(getattr(st, '_godpick_table_patch_v105', False))
        light = bool(getattr(st, '_godpick_light_checkbox_patch_v116', False))
        for kind in ('dataframe', 'data_editor'):
            active = full or (kind == 'data_editor' and light)
            if not active:
                current = getattr(st, kind)
                setattr(st, kind, _guard(current))
            # Repair saved native targets too, without stripping the bypass
            # marker from an active management wrapper.
            saved = ['_godpick_original_' + kind + '_v105']
            if kind == 'data_editor':
                saved.append('_godpick_original_data_editor_v116_light')
            for attr in saved:
                original = getattr(st, attr, None)
                if callable(original) and not getattr(original, _MARK, False):
                    setattr(st, attr, _guard(original))
        if getattr(st, '_godpick_h250r3_guard_version', '') != VERSION:
            logging.getLogger(__name__).info('%s native table guard active', VERSION)
        st._godpick_h250r3_guard_version = VERSION
    return VERSION
