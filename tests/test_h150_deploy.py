from pathlib import Path
from tools.h150_verify_deploy import verify

def test_h150_cumulative_current_repo_closure():
 ok,notes=verify(Path(__file__).resolve().parents[1])
 assert ok,notes
