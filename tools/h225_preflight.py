#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H225 cumulative exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={'.github/workflows/h123_official_institutional_daily.yml': ['85d64bf5ea81769b26dd211708232e06c384e14795f1f1d32f20358116ec6047'],
 '.github/workflows/h124_tpex_tdcc_weekly_provenance.yml': ['ed54ec86704001db397e8ca2411af283778d5aa6d4be1d327ae8d300a8d486db'],
 '.github/workflows/h190_market_archive.yml': ['1b46c7a4db80f7f5c68f19e400f804f746fed13240a44e69ddd506fb50889622',
                                               '4b7d68bc41be066be4d3d1970e08da99bd20c56b4ab8a84a243799f435dac230',
                                               '8b3fc1d54830a2d53e04725fdbfba66e1c118d98bb6131a027fbbadf2c8ad274',
                                               '966b8dfd6af8092e6c6dabb2040414040df95bc313831ce4b3da1e926f3f51c0',
                                               'ea32413519806346376ad25876fa889172d6725da055e1f777d053c13ece233a'],
 '.github/workflows/h192_financial_archive.yml': ['de1e4a7131de4b23abb01db213dee0a83db378797fcd2de3e29aeb58d3f2406b'],
 '.github/workflows/h194_daily_learning.yml': ['46c7c1514545d25875df7cd9c1241b0d92ec3ef14a59cbf6fba3619014e50e94',
                                               '68e69e384b0c087a103743cd8f2f3f838042d45bdc74c02a4fc02e572c4e99e6',
                                               '9259ed88ea615ade9e181f9f1299432e81cd440e495b7bd13b4a1818aacc63d9'],
 '.github/workflows/h195_daily_command.yml': ['460bad76a089ee843cad4aa512be9c67e67601e3ae037254975b281f26a9a846',
                                              '92b9bbcf98c1d36c7ec3a4f34bec3a08a8fe0f0edf8e541cd3c3f85ae6dad9ee'],
 '.github/workflows/h209_rolling_backtest.yml': ['b2c73c7e0a70e5f99ce1f68d2a85694b8b79114266a36b36d16e4951045086e1',
                                                 'b7715f2a321f3676f0d0b8836fcccc2c449668e7d07378908483a55093e837d4',
                                                 'c01f2ac7fc0974868af286cb2f3d0124c9cbc34b15147344cfc3a2dfd826b3a5',
                                                 'ffdd7a360861246949ea54074e6fe6dd882124f3a146ce0dbc42c2b83a921911'],
 '.github/workflows/h213_quality_gate.yml': ['f9c8a7c85b7f704786ff06881c42eb8c19fb79daffdc3a5a0909d02ba934003b', None],
 '.github/workflows/h214_universe.yml': ['16b253a52157024bc7a64eb2a52be3a6567724105b2433c506a0845d992622d9', None],
 '.github/workflows/h215_proof.yml': ['3baf6e1c077dac2dca805d82bf754576585189fcde87fd28c2876604f3f853a6', None],
 '.github/workflows/h217_quality.yml': ['f1ee04a484c357e6ba64acd98f2d2b226b2123e2bd26cffbb576e2cff3ab381d', None],
 '.github/workflows/h218_evidence.yml': ['3b1d5aeefa5274e60fb11acca8b09d89ee5f5081dc22072adac26644f9cb96f5', None],
 '.github/workflows/h219_review.yml': ['87b4a8a56dbf9fa4eaab9ac439190bb844e4ce29388710ea56a1c69ce0e20dee',
                                       '9ae839dcded0ab23d468b144ca3d15e8e13bbea7011cd934e5e4ef048320ba74',
                                       None],
 '.github/workflows/h220_review.yml': ['4857b21e25cf6986fed7715f8531f7c690c0ae0e5fefd3eb73c5ab83096adab0', None],
 '.github/workflows/h221_official_chips.yml': [None],
 '.github/workflows/h222_financial_history.yml': [None,
                                                  '6ed0054f8ec399c11c30b625e0df6771c03b46285e23e9fab0d0b7f269c879cb'],
 '.github/workflows/h223_growth_scenarios.yml': [None],
 '.github/workflows/h224_growth_resonance.yml': [None,
                                                 'a74beb2dcdca9303a8fcc83ca5155fb7fb4b9f4b92cc59876244bd2da025736c'],
 'godpick_h124_ownership_flow.py': ['0d4e54480342a93728e21da0e3d1bb9ca8fc3b0816ee9f7bc3389c4187485d1b'],
 'godpick_h190_market_archive.py': ['11c96c29395d8ce0b7514466861735873d3c75f9953eacd9ebcb1b46fc5c0c43',
                                    '23cdde524e3d2481ce58649cb934e7f6f0cf608cec665580a2aa3a699d9f9818',
                                    '623710a237fdf9a8cc09fb0a5c084470b97f8f5ec90a7eb135796d67ad8454af',
                                    'bf0e3f2be5c0e1d8bbec9106a8af00d203864fd6f8e0f724badc377728d4ac9d'],
 'godpick_h191_opportunity_engine.py': ['0bc15b9dd29b340c8de67cc0e08c44f51181bc00e83580377324214605c8be89',
                                        '39d775463d3a393adf61946cb3f2dd050ef341afa2e78bfc0ed487b56261912f',
                                        '85900f79a2852ddad6bc7cac33d7a086aa1805a1fe37a830ebe161926fa0f1f0',
                                        'bda8ca60e757cec96e00e95b386a8a00fab18a27e5ea5c8e76a67efc1fa455e8',
                                        'fcad19e1703d4d59d942a19746c8b3e806d2712c155e19358980f7c2f4c29267'],
 'godpick_h192_financial_growth.py': ['b6cd6f3f09f3319b5f962647334507e59095d2445131e8dc9c4e978b274ebd74',
                                      'cecee033eb6f2a1bba86f17e970caaea6a0363da443b8c549ad6b994cf68cac4',
                                      'adbb39225e8971f854b89bb1efea8a57b7df864cdeeb1918f06ef776c9d21d6a'],
 'godpick_h193_joint_decision.py': ['237699ff2fefc3d2b9ea71f18bf501f6e6e4d70dbab928af1967516e5ebf0c21',
                                    '3e041fc9a564224cf4c22fb144ed57165e500d177b3744c82129969126cdd0c1',
                                    '6db46b92dafff2f8e0fa876238b969ee9fe46bdee8c816a1f3be5594fc8e9147',
                                    'a586f0198e4f31a88a67ae513d9feeb5725415f4d092d72b0b985312733ab9a4'],
 'godpick_h194_performance.py': ['5aea4b1f62fbf9a254172d280229ea43705d4c9bb71c5cbeedbc7aabe35b6897',
                                 '992aff4e8975ed501c8f928dafecebb0dc6f1d3df541fbe8db145e7d97a6501e',
                                 'a2ed5197ae403d57bf9411b106bae45793117353e23aec7111ba225f4d2cf209',
                                 'c65a8b36bac0cd67ace59815c59ee98147073671dd296db82b14b707194873bb',
                                 'dd230394de66ef95c484ea3107df4b213fd924bd248b5698b9624092e2f4f0e3'],
 'godpick_h195_command_center.py': ['ace6433045a5c992b45a230fb03e74d219430957562796a3f9fdbea1dac4abc5',
                                    'd9984bb60dbf1d72ff8d3e9022b8d65d599f2f58195dc0c9a129571cc6246a19'],
 'godpick_h199_adaptive_weights.py': ['7362784f77ea774b9c38e954612dd7dd222c52460af0bbe1c8919d324526ec54',
                                      'f292a7d82cfef337da5cd428227c5a92bdb98d64166de90a3b062b280635a4c9'],
 'godpick_h206_market_gap.py': ['37bcd1722bd23683c12e4252c390e87aac06152c63ec99eb9a0d64728bf7faab',
                                '3a69390d81cebff28c89136d735f654fc62e391672cd4c82bd911182c7bd4886'],
 'godpick_h208_recall_review.py': ['404971aa98ea87a3317dd1cd1360dedf43f2c52b1ce686901a8f26f748488882',
                                   'b2214a2dbc36d0978f29bace600b6f1224a4930b5d1d3bd0368f92eb1fc89931',
                                   'fca79691cad9f4bfcbb47ada09bbf2e43aa4340c04c1809f36a895d4c3b7b559'],
 'godpick_h209_rolling_backtest.py': ['10dffde3bad519a514c7ec13498e5d295c2e51f606cb038c8b8d4e4da1f6f7e5',
                                      '110bd8fa065b387a4a2149b2e195615eba9866b4aed73fdc1262b01f066b2ccc',
                                      '1ae35ddc00ff0350cf6f4bf899a4fc81d880207a6640df0cd3fd8ffef656a31a',
                                      '402c643826ee1fef3b7aab89b975a92fd3126f12701388071908fbe0bc683af4',
                                      '4bf938d5e7ac23a1079a55859359495b0d9db9421d9f373b9fd8183a480890e8',
                                      '697e17827c00c355a25a3aab3775d80284d5b6cc8326cdd603f7a396f23e3962',
                                      'd09aaae0e4ede3a0f31a3e8a46b13c3ddd080e67e1491f4f99a6d42f2d225f7d',
                                      'e2f80e49d0906708aff4946fba5c93b7f8c18af658607aec748459ca304f055b',
                                      'fdd90b9f0fe74510f7965ef3721e9561ec3c6ac3b665149821d9a6de27bde619'],
 'godpick_h210_strategy_competition.py': ['8e4cb6ec512be00a54a7b94ad71000b62dc6b6ee863aba6d756b2ab63985f3b5', None],
 'godpick_h211_counterevidence.py': ['16b789908593f416b2ad0cbf7bcffba88498c3e92bc1f66a12f6e784b0daa66b',
                                     '19ee71b2bc7f6b486c3f311ab34c9b1d29dba5ebfc1ad1509e30d1e121f0984b',
                                     None],
 'godpick_h212_data_recovery.py': ['83dd4bf113628cb92908d7a1baf3aaa5b45619d5e9b2cbe256db8845c15c6370',
                                   '88545b287504bfe70a0bdadd261547346dc3041a0170b0b1b577282d4ed8097f',
                                   'f4677be2fdf8961ec7f9cfe83cff6699affe8ddf798c5c7c874ad0b24e273553',
                                   None],
 'godpick_h213_quality_gate.py': ['323fe166b58be0b4988b715e043e30dca645cc794cefe86858c447f091dd1812',
                                  'af0bff3c7d135bc9d2b3280106e919b86f597ef2ae879f9d5540fdf63728ccda',
                                  'b0ea1877515483b8cff76cb5a796e0264eb6bf66fea026d7178bb350451c594f',
                                  None],
 'godpick_h214_universe.py': ['540effa853c6f1de68221dd607f1f93491413a894a3554bc6f9354ff642ce0cd',
                              'bfb56529e292b7c7437d34b776568b2f96ddadf9f1f6f4e5b5c4585230a00aa5',
                              None],
 'godpick_h215_proof.py': ['07d996f2cd93c983fea0c59b532f70c33ed8e43d1369371263c99e632cbec6dd',
                           '0b6a02bf8a4e86b3e8a33014f4a29d24f9a298b0272006cfae7e702c55a37145',
                           None],
 'godpick_h217_prospective_quality.py': ['dbaf18a83b362054667b271ed837c525340a853edf8e6dde909cbf565dd1410c', None],
 'godpick_h218_evidence.py': ['455f48662e92f967b417cde9b23fad0801d21a198b72313727396b7df79fadb1', None],
 'godpick_h219_first_review.py': ['5c2518ae0b0a77cad0fc0a23911722d571809568adf235f3a01bac036d380e2b', None],
 'godpick_h220_growth_rotation.py': ['97adab9d9b1865b0328ec117d1dc81499163bfdab3545507015a00fe59df24cb',
                                     None,
                                     'ec15c30cb081d095724f1ffed61e2d25b49bc8ce56f34e433ba507a94a5172b2'],
 'godpick_h221_evidence_closure.py': [None],
 'godpick_h222_financial_history.py': [None, '4a5b1465a765625ecc0f649a85c1cb31a05e2a9512db8b42e265c3bd290e336a'],
 'godpick_h223_growth_scenarios.py': [None],
 'godpick_h224_growth_resonance.py': [None],
 'godpick_h225_integrated_quality.py': [None],
 'pages/32_隔日預測與AI績效學習.py': ['05fc50fd7fbb52e7e1721000102f53d541bc26f3cff08bf83bc1b0581b6e26ac',
                             '06b1299f382cd4af23a5e2b593e8549719d46994a07e87d829743495af8c4085',
                             '3425b432cfd24e155ba3e8d9863ee1548f377427d3240f64bc3667c26622b720',
                             '41e25ec8b5c52ba77505a3063c3c870cc61bc0fd7cefef7917775efa0ec66ca2',
                             '51f6237d89cea76985c16808f2e06b51148687153a7567826c0a9084505a7abd',
                             '6abb02f0ea2c6174b69c3d6f927efbcc6f56cd652195ce49ff6b44d8ea711762',
                             '6be30f3fba0e0559dadeb8a8dfe36264dfa01761fcbd11c1194f34c0b8f732d4',
                             '6e1845a0c80ddf21d15481d4915dca8420b2ec8c06d0f9def0582ac790d8148a',
                             '713701756391053295ea6df692c4be7e5771ff17d3d94f9c6c6bcaf593b43885',
                             '802416d4e20c0cb49c228dd72e220efc86df230d5008ebb8aa1203afc13cc5a9',
                             '9bd3c3a84d28e5aff0086bd6b237f4ca5c432b2526c05d63b633780c4d441ef7',
                             'df2246d3b7a03be9566421feba0fb2eb34f7911f9c5ee98c8ab6da8a1e085de3',
                             '11a41a626012fb63408ddae7e0eeee4019486548ef44e1bf651e2595902f4179',
                             'f84c4d043e45a5c170260ee4df8f1cb70662aa39735bbe94c799e64e49cc619a',
                             'd929669e07f4fd463684dd7ee733ce533fc5f4586935a98089774457f36cf621',
                             '4638d0424b22780502ca0de0f869317053d44ea8332335d5e2a9a9de6c9498e5'],
 'pages/33_超級AI股神決策總指揮中心.py': ['07d7706f10a97cde4350c7357aef9671fc320bfcab4233d92828b029fd0d7157',
                               'a8ca8bba6262b7a4ef2ac5d96a0862cf371d54692aea10d12699eb0db6e7ce80'],
 'pages/7_股神推薦.py': ['041059a53b4048b727cae3882ef0ef41d494b2027aadaff2c729d04d7644dde5',
                     '061c8809e6cb2a9fc14c691ee357d98225d9242d5d10c5159233c222343c6e31',
                     'b843a741b1758c818adf8e33e980b285c599f5ef6972c757345fbb9a1c65322e',
                     'f81e33fd1958fae90eeaac2d18107953ed926fd71d7529f52b480066b80cdf56',
                     '4d0385a6116f10c766dc9d64ee4d10750ba9e105be3f170f34137bfa90027be1',
                     '7ec4d5505699dc0c74c3866316ab8b84fd0f11456eafd97dec6f4bea6aa0153f'],
 'tests/test_h199_adaptive_weights.py': ['16114e72894d69da9e498898bb4e50cc201517850f813676b4e62141810e6131',
                                         '2ca092960e98e552d5a4d3747c6058f013a8db74b10ab9b617b8ea828b2f4654'],
 'tests/test_h2091_regression.py': ['53f4b292c46aca939eedfb58cc0609fd950101eedfaf426a02e49b6121c94ae1', None],
 'tests/test_h210_strategy_competition.py': ['a5ca24fd6d20f24cb452b343d96ba5642dce1d35851a4c26e8204791c2c9100d', None],
 'tests/test_h211_counterevidence.py': ['ad3b1ed811d1c34740fe5ef4bfb3852d96c1103b33e43a7de47259b1d499dccc', None],
 'tests/test_h212_data_recovery.py': ['f26b78cdbcea3119b59912a8da88b27ccf12d753f355b7da62e5032cf7cf067a', None],
 'tests/test_h213_quality_gate.py': ['3abd65f0236c823684f680a55ba763f713e81b485ba78a4e83bf0a7bd20f0f6b', None],
 'tests/test_h214_universe.py': ['e9a8c75bbbd6f37a9cb8826cf175c13867afe832fa62ddc27af626e21f4fdad9', None],
 'tests/test_h215_proof.py': ['3e9a99167942946a52898e7f1654737d3056f672396a2f47f61880371cd55735', None],
 'tests/test_h216_pipeline.py': ['428b91391d8ad8f402e6c890b74a5e3c2360bab10e9e5434ebacafa49e657efa', None],
 'tests/test_h217_prospective_quality.py': ['47e4616238573e7f1b3a25dee9f67c358b721381aeb5595ee99b29b367f20da0', None],
 'tests/test_h218_evidence.py': ['537f168de78ac237e92488b63af6aa35cebc9af4aff93b3f440d3aaf974de1e0', None],
 'tests/test_h219_first_review.py': ['591ed36f126eab365b19c8efe423f2ffacc087629e6838e05fbf5a887010f840', None],
 'tests/test_h220_growth_rotation.py': ['45f3f2a31f470f300aefad77fb1bbb5a20b15399d52ae0c954756e6d9b08c3ae', None],
 'tests/test_h221_evidence_closure.py': [None],
 'tests/test_h222_financial_history.py': [None],
 'tests/test_h223_growth_scenarios.py': [None],
 'tests/test_h224_growth_resonance.py': [None],
 'tests/test_h225_integrated_quality.py': [None],
 'tools/h190_market_archive.py': ['3e043bbfb4a3725cd746b5f710bc0f2dc2dfef7a94c0ee0ba6a0e30d9d5b3bc7',
                                  '4af5cbcdf05aea3b731c26bd4bc7836bc155d527b4f1dcd17856480118ce006c'],
 'tools/h194_daily_learning.py': ['15fd27f67d54892786caefd851d5c68b251e0f672b795e3a7efe8f4159cf925c',
                                  '5af9d31c53f89708aaaaf53e3bc28fb710b338f95da9bfadbe19f4b725532f54'],
 'tools/h195_daily_command.py': ['321cc287d5127e8e716909325114db757b0d9ff1a6ce79ff1cf6cc4b7e4b0776',
                                 '46227312d47e0d2b939d7e910b705f42fe8a529b4b44d01d54f2d63d74425df6'],
 'tools/h210_daily_competition.py': ['dd44e73b1fcf3db9a96374a90b34e62d40cf841b432e76d309a4874b4f0cfa54', None],
 'tools/h212_data_recovery.py': ['7b17c45ce81f87c3b85039b663e46f9acb6d8476f4e373d0f809e1621d5f357d', None],
 'tools/h213_dispatch.py': ['df4fa680af381d4dafeef225c6e478017daa1e7dc50e719a96cc04cffe38ab21', None],
 'tools/h213_quality.py': ['f453a846034a9b6ec52dfd9a1352705b25d4abaf6c35ad0d2f965877dba90191', None],
 'tools/h214_universe.py': ['75ee693651fd4afd6ec946b9536623348cea2c006a9c472d2a75ed44badd4c31', None],
 'tools/h215_proof.py': ['dd34b3e34c6ca9f8f419453a8543dad59fc102524ce2c0d9ea406dbafcbdd510', None],
 'tools/h217_quality.py': ['4c4f0a836849544eed0b223f5f8e7cef90e0825258b45a8bf0041e496669f4e1', None],
 'tools/h218_evidence.py': ['97b461256290e3ccbaa5bdcabdd62bba6c1277f2080eac41f875766f25b3d92c', None],
 'tools/h219_review.py': ['3855de713e9a5537073b72ad106cf1523f8bef3af3cebda996820bc993a7187a', None],
 'tools/h220_review.py': ['4ecac9dc8b33180164a48ea8f926660dfe25815c102b6b31d6c52d5732b97ca8', None],
 'tools/h221_collect.py': [None],
 'tools/h222_history.py': [None],
 'tools/h223_review.py': [None],
 'tools/h224_review.py': [None],
 'tools/h225_review.py': [None]}
DEPENDENCIES={'godpick_h123_flow_provenance.py': '9948d40b2796292ab63222fe8672e25c9e0b04796242bf05d9bd6ee1edd98174',
 'godpick_h197_strategy_review.py': '46d0b4e12463d5791ff7ccb3240e06b00c3608a3c4d46afe242c784d14910218',
 'godpick_h198_stage_review.py': '248db22227ffd8d82a9811f5ed97b12571720d1978ee3d3ab7c78dfdb2c27a50',
 'tools/h209_daily_backtest.py': '8a83d639762d28b373b3ffd8b5cb9d5af5a8947790ab054289026ae589487aac'}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h225-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H225_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or sha(p.read_bytes())!=want:errors.append('H225_DEPENDENCY_MISMATCH: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H225'
        elif (None if before is None else sha(before)) in base:state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h225_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
    prior={rel:(root/rel).read_bytes() if (root/rel).is_file() else None for rel in todo}
    for rel,b in prior.items():
        if b is not None:atomic(backup/rel,b)
    try:
        for rel in todo:
            p=root/rel
            if (p.read_bytes() if p.is_file() else None)!=prior[rel]:raise RuntimeError('concurrent change: '+rel)
        for rel in todo:atomic(root/rel,(payload/rel).read_bytes())
        _,left,errs=check(root,payload)
        if left or errs:raise RuntimeError('post-install check failed')
    except Exception:
        for rel,b in prior.items():
            p=root/rel
            # Only undo our own bytes; don't overwrite unexpected concurrent updates.
            if p.is_file() and p.read_bytes()==(payload/rel).read_bytes():
                if b is None:p.unlink()
                else:atomic(p,b)
        raise
    return backup

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    root=a.root.resolve();payload=Path(__file__).resolve().parents[1]
    details,todo,errors=check(root,payload);print(json.dumps({'details':details,'errors':errors},ensure_ascii=False,indent=2))
    if errors:return 3
    if not todo:print('ALREADY_APPLIED_H225');return 0
    if not a.apply:print('READY_APPLY_H225');return 0
    print('APPLIED_H225 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
