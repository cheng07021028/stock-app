"""Deployment dependency guard for Streamlit 1.59.2.

Run after dependencies are installed:
    python tests/phase105_1_dependency_guard.py
"""
from __future__ import annotations

import inspect

import starlette
import streamlit
from starlette.middleware.gzip import GZipResponder

EXPECTED_STREAMLIT = "1.59.2"
EXPECTED_STARLETTE = "1.3.1"


def main() -> None:
    assert streamlit.__version__ == EXPECTED_STREAMLIT, (
        f"Unexpected Streamlit: {streamlit.__version__}"
    )
    assert starlette.__version__ == EXPECTED_STARLETTE, (
        f"Unexpected Starlette: {starlette.__version__}"
    )
    signature = inspect.signature(GZipResponder.__init__)
    assert "thread_minimum_size" not in signature.parameters, (
        "Incompatible Starlette GZipResponder signature detected: "
        f"{signature}"
    )
    print(
        "PASS: Streamlit/Starlette GZip compatibility guard | "
        f"streamlit={streamlit.__version__} | starlette={starlette.__version__} | "
        f"signature={signature}"
    )


if __name__ == "__main__":
    main()
