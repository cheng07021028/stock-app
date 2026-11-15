# -*- coding: utf-8 -*-
"""Offline H162 GitHub authorization diagnostics and H161 token source parity."""
from __future__ import annotations
import ast
import base64
import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h162_github_auth_diagnostic import (
    resolve_config, public_config, run_readonly_probe, run_write_probe,
)

TOKEN_A = "github_pat_aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "github_pat_bbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
S = {"GITHUB_TOKEN": TOKEN_A, "GITHUB_REPO_OWNER": "cheng07021028",
     "GITHUB_REPO_NAME": "stock-app", "GITHUB_RUNTIME_DATA_BRANCH": "runtime-data"}
E = {"GITHUB_TOKEN": TOKEN_B, "GITHUB_REPO_BRANCH": "main",
     "GITHUB_RUNTIME_DATA_BRANCH": "runtime-data"}


class Response:
    def __init__(self, code, obj=None):
        self.status_code = code
        self.obj = obj or {}
    def json(self):
        return self.obj


class FakeGitHub:
    def __init__(self, *, allow_write=True, allow_read=True, allow_delete=True):
        self.allow_write = allow_write
        self.allow_read = allow_read
        self.allow_delete = allow_delete
        self.file = None
        self.calls = []
        self.headers = []
        self.data_path = ""
        self.file_sha = "probe-sha"

    def get(self, url, **kw):
        self.calls.append(("GET", url))
        self.headers.append(kw.get("headers", {}))
        if not self.allow_read:
            return Response(403)
        if "/contents/" in url:
            if self.file is None:
                return Response(404)
            return Response(200, {"sha": self.file_sha,
                                  "content": base64.b64encode(self.file).decode("ascii")})
        if url.endswith("/branches/runtime-data"):
            return Response(200, {"name": "runtime-data"})
        if url.endswith("/stock-app"):
            return Response(200, {"full_name": "cheng07021028/stock-app"})
        return Response(404)

    def put(self, url, **kw):
        self.calls.append(("PUT", url))
        self.data_path = url
        self.headers.append(kw.get("headers", {}))
        if not self.allow_write:
            return Response(403)
        body = kw["json"]
        assert body["branch"] == "runtime-data"
        self.file = base64.b64decode(body["content"])
        return Response(201, {"content": {"sha": self.file_sha}})

    def delete(self, url, **kw):
        self.calls.append(("DELETE", url))
        self.headers.append(kw.get("headers", {}))
        if not self.allow_delete:
            return Response(403)
        if self.file is None or kw["json"]["sha"] != self.file_sha:
            return Response(409)
        self.file = None
        return Response(200)


class DiagnosticTests(unittest.TestCase):
    def test_secrets_priority_and_conflict_detected(self):
        cfg = resolve_config(secrets=S, environ=E)
        self.assertEqual(cfg["token"], TOKEN_A)
        self.assertEqual(cfg["source"], "Streamlit Secrets")
        self.assertTrue(cfg["sources_conflict"])
        self.assertEqual(cfg["branch"], "runtime-data")

    def test_environment_fallback_for_workers(self):
        cfg = resolve_config(secrets={}, environ=E)
        self.assertEqual(cfg["token"], TOKEN_B)
        self.assertEqual(cfg["source"], "Environment")

    def test_no_token_or_fingerprint_in_public_config(self):
        cfg = resolve_config(secrets=S, environ=E)
        report = public_config(cfg)
        data = json.dumps(report, ensure_ascii=False)
        self.assertNotIn(TOKEN_A, data)
        self.assertNotIn(TOKEN_B, data)
        self.assertNotIn("token", data.lower())
        self.assertTrue(report["兩處憑證不同"])

    def test_read_only_no_put_or_delete(self):
        backend = FakeGitHub()
        report = run_readonly_probe(secrets=S, environ=E, transport=backend)
        self.assertTrue(report["ok"], report)
        self.assertEqual([c[0] for c in backend.calls], ["GET", "GET"])
        self.assertNotIn(TOKEN_A, json.dumps(report))

    def test_read_denied_stops_write(self):
        backend = FakeGitHub(allow_read=False)
        report = run_write_probe(secrets=S, environ=E, transport=backend)
        self.assertFalse(report["ok"])
        self.assertFalse(any(v[0] == "PUT" for v in backend.calls))

    def test_http_403_write_is_not_success(self):
        backend = FakeGitHub(allow_write=False)
        report = run_write_probe(secrets=S, environ=E, transport=backend)
        self.assertFalse(report["ok"])
        self.assertTrue(any(x["HTTP"] == "403" for x in report["steps"]))
        self.assertFalse(any(c[0] == "DELETE" for c in backend.calls))
        self.assertIsNone(backend.file)

    def test_write_verify_delete_roundtrip(self):
        backend = FakeGitHub()
        report = run_write_probe(secrets=S, environ=E, transport=backend)
        self.assertTrue(report["ok"], report)
        self.assertIsNone(backend.file)
        self.assertEqual(report["probe_path"], "")
        self.assertEqual([c[0] for c in backend.calls], ["GET", "GET", "PUT", "GET", "DELETE", "GET"])
        self.assertIn("/contents/data/.h162_auth_probe_", backend.data_path)
        self.assertNotIn("godpick_records", backend.data_path)
        self.assertNotIn(TOKEN_A, json.dumps(report, ensure_ascii=False))

    def test_failed_cleanup_is_never_success_and_exposes_only_probe_path(self):
        backend = FakeGitHub(allow_delete=False)
        report = run_write_probe(secrets=S, environ=E, transport=backend)
        self.assertFalse(report["ok"])
        self.assertTrue(report["probe_path"].startswith("data/.h162_auth_probe_"))
        self.assertIsNotNone(backend.file)
        self.assertNotIn(TOKEN_A, json.dumps(report))

    def test_code_branch_is_not_probe_target(self):
        backend = FakeGitHub()
        report = run_write_probe(secrets={**S, "GITHUB_RUNTIME_DATA_BRANCH": "main"}, environ={},
                                 transport=backend)
        self.assertFalse(report["ok"])
        self.assertEqual(backend.calls, [])

    def test_missing_token_cannot_pass(self):
        backend = FakeGitHub()
        report = run_readonly_probe(secrets={}, environ={}, transport=backend)
        self.assertFalse(report["ok"])
        self.assertEqual(backend.calls, [])

    def test_h161_secret_precedence_is_executable_and_keeps_branch_guard(self):
        code = (ROOT / "godpick_persistence_service.py").read_text(encoding="utf-8")
        module = ast.parse(code)
        fn = next(x for x in module.body if isinstance(x, ast.FunctionDef) and x.name == "_secret")
        isolated = ast.Module(body=[fn], type_ignores=[])
        st = types.SimpleNamespace(secrets={"GITHUB_TOKEN": TOKEN_A, "GITHUB_RUNTIME_DATA_BRANCH": "runtime-data"})
        env = {"GITHUB_TOKEN": TOKEN_B, "GITHUB_RUNTIME_DATA_BRANCH": "runtime-data", "GITHUB_REPO_BRANCH": "main"}
        ns = {"st": st, "os": types.SimpleNamespace(getenv=lambda k, default="": env.get(k, default)),
              "_safe_str": lambda x: str(x or "").strip()}
        exec(compile(isolated, "<H162-secret-test>", "exec"), ns)
        self.assertEqual(ns["_secret"]("GITHUB_TOKEN"), TOKEN_A)
        self.assertEqual(ns["_secret"]("GITHUB_RUNTIME_DATA_BRANCH"), "runtime-data")
        st.secrets = {}
        self.assertEqual(ns["_secret"]("GITHUB_TOKEN"), TOKEN_B)

    def test_admin_only_page_ui_and_no_snapshot_mutation(self):
        page = (ROOT / "pages" / "17_系統健康檢查.py").read_text(encoding="utf-8")
        ast.parse(page)
        self.assertIn('auth_role', page)
        self.assertIn('!= "admin"', page)
        self.assertIn('h162_confirm_temporary_contents_probe', page)
        self.assertIn('_h162_write_probe(secrets=st.secrets)', page)
        self.assertNotIn('write_records_firestore_mutation(', page)
        self.assertNotIn('save_records_mutation_fast(', page)


if __name__ == "__main__":
    unittest.main(verbosity=2)
