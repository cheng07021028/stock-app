import tempfile, unittest, zipfile, json
from pathlib import Path
from unittest.mock import patch
import BUILD_H250 as b

class Builder(unittest.TestCase):
    def test_stage_map(self):
        base,steps=b.stages();self.assertEqual(len(base),21);self.assertEqual(sum(len(p) for v in steps.values() for _,p in v),78)
        self.assertEqual([n for n,_ in steps['pages/17_系統健康檢查.py']],[244,248,249])
    def test_exact_original(self):
        raw=b'x=1\n';self.assertEqual(b.recover(raw,b.blob(raw),[(244,[('x=1','x=2')])]),('x=1\n',0))
    def test_exact_stages(self):
        raw=b'x=1\n';steps=[(244,[('x=1','x=2')]),(248,[('x=2','x=3')])]
        for i,v in enumerate((raw,b'x=2\n',b'x=3\n')):self.assertEqual(b.recover(v,b.blob(raw),steps),('x=1\n',i))
    def test_modified_base_rejected(self):
        with self.assertRaises(ValueError):b.recover(b'x=2\ny=9\n',b.blob(b'x=1\n'),[(244,[('x=1','x=2')])])
    def test_crlf(self):self.assertEqual(b.recover(b'x=1\r\n',b.blob(b'x=1\n'),[]),('x=1\n',0))
    def test_bom(self):
        raw='\ufeffx=1\n'.encode();self.assertEqual(b.recover(b'x=2\n',b.blob(raw),[(245,[('x=1','x=2')])]),(raw.decode(),1))
    def test_anchor_ambiguous(self):
        with self.assertRaises(ValueError):b.change('x x',[('x','y')])
    def test_generate_and_no_original_change(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p=root/'base.py';p.write_bytes(b'x=1\n');out=root/'out.zip';base={'base.py':b.blob(p.read_bytes())};steps={'base.py':[(244,[('x=1','x=2')]),(249,[('x=2','x=3')])]}
            with patch.object(b,'stages',return_value=(base,steps)):
                m,_=b.generate(root,out)
                self.assertEqual(p.read_bytes(),b'x=1\n');self.assertEqual(m['files']['base.py']['sha256'],b.sha(b'x=3\n'))
                with zipfile.ZipFile(out) as z:self.assertEqual(z.read('base.py'),b'x=3\n');self.assertIsNone(z.testzip());self.assertNotIn('data/', '\n'.join(z.namelist()))
                with self.assertRaises(ValueError):b.generate(root,out)
    def test_conflict_no_zip(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'base.py').write_bytes(b'x=99\n');out=root/'out.zip'
            with patch.object(b,'stages',return_value=({'base.py':b.blob(b'x=1\n')},{'base.py':[(244,[('x=1','x=2')])]})):
                with self.assertRaises(ValueError):b.generate(root,out)
            self.assertFalse(out.exists())
if __name__=='__main__':unittest.main()
