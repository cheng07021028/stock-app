import hashlib,sys,tempfile,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import APPLY_H250R5 as b
class Builder(unittest.TestCase):
    def test_unique_anchor(self):self.assertEqual(b.replace('old','old','new','test'),'new')
    def test_missing_anchor(self):self.assertRaises(ValueError,b.replace,'old','other','new','test')
    def test_duplicate_anchor(self):self.assertRaises(ValueError,b.replace,'old old','old','new','test')
    def test_exact_recovery(self):
        raw=b'old\n';data={'blob':b.blob(raw),'steps':[[244,[['old','new']]]]}
        with patch.dict(b.BASE,{'test.py':data}):
            self.assertEqual(b.recover(raw,'test.py'),('old\n',0));self.assertEqual(b.recover(b'new\n','test.py'),('new\n',1));self.assertRaises(ValueError,b.recover,b'other\n','test.py')
    def test_overlay_unicode(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'overlay.zip';b.build_zip({'pages/17_系統健康檢查.py':b'x=1\n'},p)
            with zipfile.ZipFile(p) as z:self.assertIsNone(z.testzip());self.assertEqual(z.read('pages/17_系統健康檢查.py'),b'x=1\n')
    def test_apply_backup(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)/'repo';root.mkdir();(root/'x.py').write_bytes(b'x=1\n');bak=Path(t)/'backup'
            b.apply(root,{'x.py':b'x=2\n','new.py':b'pass\n'},{'x.py':b'x=1\n'},bak)
            self.assertEqual((bak/'x.py').read_bytes(),b'x=1\n');self.assertEqual((root/'x.py').read_bytes(),b'x=2\n')
    def test_reject_changed_after_prepare(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)/'repo';root.mkdir();(root/'x.py').write_bytes(b'x=3\n')
            with self.assertRaises(ValueError):b.apply(root,{'x.py':b'x=2\n'},{'x.py':b'x=1\n'},Path(t)/'backup')
            self.assertEqual((root/'x.py').read_bytes(),b'x=3\n')
if __name__=='__main__':unittest.main()
