# -*- coding: utf-8 -*-
from pathlib import Path
import ast
import argparse
import importlib.util

def verify(root:Path) -> None:
    files=[
      'pages/7_股神推薦.py','godpick_h1534_three_margin.py',
      'godpick_h115_pit_evidence.py','godpick_h153_growth_setup_rank.py',
      'godpick_auto_scheduler.py','godpick_auto_update_tasks.py',
      'godpick_history_sources.py','pages/10_推薦清單.py',
      '.github/workflows/h1534_official_margins.yml',
      'tools/h1534_import_margins.py',
      'tests/test_h1534_three_margin.py'
    ]
    missing=[f for f in files if not (root/f).is_file()]
    if missing:raise RuntimeError('missing H153.4 dependencies: '+','.join(missing))
    for name in ('godpick_h115_pit_evidence.py','godpick_h153_growth_setup_rank.py',
                 'godpick_h1534_three_margin.py','pages/7_股神推薦.py'):
        ast.parse((root/name).read_text('utf-8'),filename=name)
    assert 'append_quarterly_margins' in (root/'godpick_h115_pit_evidence.py').read_text('utf-8')
    assert 'evaluate_three_rises' in (root/'godpick_h153_growth_setup_rank.py').read_text('utf-8')
    assert 'H1534三率三升狀態' in (root/'godpick_h153_growth_setup_rank.py').read_text('utf-8')
    assert "tables['h126_authority']" in (root/'pages/7_股神推薦.py').read_text('utf-8')
    assert 'H153.3' in (root/'README_H1533.md').read_text('utf-8')
    print('PASS H153.4 official three-rises PIT + H153.3 cumulative dependency')

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--root',type=Path,default=Path('.'))
    verify(a.parse_args().root)
