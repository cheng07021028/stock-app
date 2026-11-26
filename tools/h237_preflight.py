#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H237 cumulative exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={'.github/workflows/h123_official_institutional_daily.yml': ['85d64bf5ea81769b26dd211708232e06c384e14795f1f1d32f20358116ec6047',
                                                             '9cd9a500c17d4118912e82d20bb2cb5e2c8c707970dd9bc2266b86bacb09fb43'],
 '.github/workflows/h124_tpex_tdcc_weekly_provenance.yml': ['ed54ec86704001db397e8ca2411af283778d5aa6d4be1d327ae8d300a8d486db',
                                                            'c0260076dd1bdc1d05b62793808663e814f258c3e3b594fa5cc2379f38e3b453'],
 '.github/workflows/h190_market_archive.yml': ['1b46c7a4db80f7f5c68f19e400f804f746fed13240a44e69ddd506fb50889622',
                                               '4b7d68bc41be066be4d3d1970e08da99bd20c56b4ab8a84a243799f435dac230',
                                               '8b3fc1d54830a2d53e04725fdbfba66e1c118d98bb6131a027fbbadf2c8ad274',
                                               '966b8dfd6af8092e6c6dabb2040414040df95bc313831ce4b3da1e926f3f51c0',
                                               'ea32413519806346376ad25876fa889172d6725da055e1f777d053c13ece233a'],
 '.github/workflows/h192_financial_archive.yml': ['de1e4a7131de4b23abb01db213dee0a83db378797fcd2de3e29aeb58d3f2406b',
                                                  '2725f5f03805fb16c2ee7f2d71f4c876d913d6b992ccc7123d9aab47978e6f4c'],
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
 '.github/workflows/h221_official_chips.yml': [None,
                                               '3606c19427d146124afaae9a489ff5d4e9a078e6819709e7fe292decdcccbdd9',
                                               'b3d9ebe1ae9c17cdf1a44c439aefdfa78becc92d9d06cc65eae6243109493d94'],
 '.github/workflows/h222_financial_history.yml': [None,
                                                  '6ed0054f8ec399c11c30b625e0df6771c03b46285e23e9fab0d0b7f269c879cb',
                                                  '9cb84ebe6a4daa6c8e480ae5e5b5ec65bcc72115e334177e3213b27597cca95b',
                                                  '03c411271524067cc66a5e4975a5093cde58e17ab5e38b2a12000ef71d708ada',
                                                  'b3a782154352a7e70bf24e731bfe68b70ae5f5bd1d00d88f23f9f2abfef916ec',
                                                  '1eacba17c56f03dc49e1ad1cece35083bd2a3ad7857fdb75185e129838d3a489',
                                                  'ac69714b3a7c388745f888e46072acfbceafaa99b5bba49b1912de5297fb8be6'],
 '.github/workflows/h223_growth_scenarios.yml': [None, '1cf620c526abb524de0af652dcb28cfec2332e8ae5ace5f00121bb2ed0beaae6'],
 '.github/workflows/h224_growth_resonance.yml': [None,
                                                 'a74beb2dcdca9303a8fcc83ca5155fb7fb4b9f4b92cc59876244bd2da025736c',
                                                 '66bf3c0360f6bab877e47c635cca5dcfd7e839a96cb0cc396a302dd522bd58a7',
                                                 'efd6e667e42940bb74c5921341572c1fe8e8cedfa16cf66b416e43f0cedd648e'],
 'godpick_h124_ownership_flow.py': ['0d4e54480342a93728e21da0e3d1bb9ca8fc3b0816ee9f7bc3389c4187485d1b',
                                    '0d56509cfde0916d2f95d2bd2836afc2668c06a6bc50cf6340347affdb72ed07'],
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
                                      'adbb39225e8971f854b89bb1efea8a57b7df864cdeeb1918f06ef776c9d21d6a',
                                      '151a63dfc1e905a3b6263c681bd1ebbd8df6ecb0078840811cf136f8bc2cc8e6',
                                      '72f9626160e7f224be31b2d0f15bdc1b6dbcaa21c5908398a9669d6f9bfccf24',
                                      '1610df750ab00df7fc2b33115f6c653298af9cfd9bfa8d241ffab1a18f6020aa'],
 'godpick_h193_joint_decision.py': ['237699ff2fefc3d2b9ea71f18bf501f6e6e4d70dbab928af1967516e5ebf0c21',
                                    '3e041fc9a564224cf4c22fb144ed57165e500d177b3744c82129969126cdd0c1',
                                    '6db46b92dafff2f8e0fa876238b969ee9fe46bdee8c816a1f3be5594fc8e9147',
                                    'a586f0198e4f31a88a67ae513d9feeb5725415f4d092d72b0b985312733ab9a4'],
 'godpick_h194_performance.py': ['5aea4b1f62fbf9a254172d280229ea43705d4c9bb71c5cbeedbc7aabe35b6897',
                                 '992aff4e8975ed501c8f928dafecebb0dc6f1d3df541fbe8db145e7d97a6501e',
                                 'a2ed5197ae403d57bf9411b106bae45793117353e23aec7111ba225f4d2cf209',
                                 'c65a8b36bac0cd67ace59815c59ee98147073671dd296db82b14b707194873bb',
                                 'dd230394de66ef95c484ea3107df4b213fd924bd248b5698b9624092e2f4f0e3',
                                 '841c4b1b787deb961cf3bc8c99395c9a036a0109d08c62a87194d1ea9b05cd43',
                                 'a09392fd997438d3c4e1780591338a3664f51bcb4c7eddf7bcf02c797a0b44e8'],
 'godpick_h195_command_center.py': ['ace6433045a5c992b45a230fb03e74d219430957562796a3f9fdbea1dac4abc5',
                                    'd9984bb60dbf1d72ff8d3e9022b8d65d599f2f58195dc0c9a129571cc6246a19'],
 'godpick_h199_adaptive_weights.py': ['7362784f77ea774b9c38e954612dd7dd222c52460af0bbe1c8919d324526ec54',
                                      'f292a7d82cfef337da5cd428227c5a92bdb98d64166de90a3b062b280635a4c9'],
 'godpick_h206_market_gap.py': ['37bcd1722bd23683c12e4252c390e87aac06152c63ec99eb9a0d64728bf7faab',
                                '3a69390d81cebff28c89136d735f654fc62e391672cd4c82bd911182c7bd4886',
                                '73477cbfa4cb2b1a603dd38d8f6949a15a8de3b987bf22db63dbb8cd425ea17a'],
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
                                      'fdd90b9f0fe74510f7965ef3721e9561ec3c6ac3b665149821d9a6de27bde619',
                                      'ed00b345068e3bae6fc261815f5be7e03766e77ab0b31c99ab3140940a998d2f'],
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
 'godpick_h217_prospective_quality.py': ['dbaf18a83b362054667b271ed837c525340a853edf8e6dde909cbf565dd1410c',
                                         None,
                                         '3ebd4db3068d945c74e8a9f51596d21e3141028d613e0f215b0d08a55b594357'],
 'godpick_h218_evidence.py': ['455f48662e92f967b417cde9b23fad0801d21a198b72313727396b7df79fadb1',
                              None,
                              '0c5a8a9919367b3bff94f64564f3b6e4df15d07e97f75b96191cd30a0cf41fd2'],
 'godpick_h219_first_review.py': ['5c2518ae0b0a77cad0fc0a23911722d571809568adf235f3a01bac036d380e2b',
                                  None,
                                  'ddb3f05d5f4ab79564be89be8aada6dcc03a27489c31a7e2d85be8c3a70e02c8'],
 'godpick_h220_growth_rotation.py': ['97adab9d9b1865b0328ec117d1dc81499163bfdab3545507015a00fe59df24cb',
                                     None,
                                     'ec15c30cb081d095724f1ffed61e2d25b49bc8ce56f34e433ba507a94a5172b2',
                                     '38cee6e35e6c667a645be5be97bcd9d15dedbfca9f68e5941473747b8c383411'],
 'godpick_h221_evidence_closure.py': [None, '7b7491c6716df9ea9031a975087073e165ad4aaf1ada13fca23a1a84f7029ba0'],
 'godpick_h222_financial_history.py': [None,
                                       '4a5b1465a765625ecc0f649a85c1cb31a05e2a9512db8b42e265c3bd290e336a',
                                       '9b55a7381b6b5345cb9f478a93a44ffe834d8027a374605533401d9666f2ae9b',
                                       'cb866669b47d8a0b41a019e40b34014917cb1f6bee49812d27e7e2e023a12357',
                                       '61875ea7e579773a4bffba0ba2756a281c0a8f8613638bec83809c02883249a7',
                                       '9f4935785b094ae9f8e95483df064e4bfec11f8321d9d788ddedfb88bf7eef45',
                                       '25209ba8f9580e43e55dfa2b392b975ccff859321467730af9ec582c29ab2fd9'],
 'godpick_h223_growth_scenarios.py': [None,
                                      '2cbd768160d950c0130fda2228364865ffd14cc69d4d0c151c67cadfd7684076',
                                      'ff17b880c5620cd0cb5414b2ecd420a307041ee5eeecd5b40dacd4aa4e6abbd3',
                                      '9e8ce9e99398e7bef389ad296f91a2bbf7c662675c8fefbf9b9662e8f1e5836c'],
 'godpick_h224_growth_resonance.py': [None,
                                      'de269abc36959cbc9a47e0c426517cbc69022922889f3206080971228f923a37',
                                      'fc293e675dd67d4e49daedcef546decb58a957bf60f9dea33fa328d14ac2097f'],
 'godpick_h225_integrated_quality.py': [None,
                                        'd40a3c907942f958ad541183daa6edf73dd1891ddce252b8345fac9f01f4f0ee',
                                        'e2a72036ffafb9748dc5e58adf88ca76ad02642b9fe7c25e4c86b4c59169e650',
                                        'e90a5ebe9b085562f7dc9c9cfba3489cb872562fb4fa68a13e31f96a8b221add',
                                        'a56aa518eee08d5ab82252a892ba43fb2e98d10012ff5b3dd646baabb707b017'],
 'godpick_h226_live_acceptance.py': [None, '0a32f4d6890611d463f0d1e2b04f58d69e6e5926fa09a8797da0fc707cab4b1a'],
 'godpick_h227_strategy_validation.py': [None, '65de3a1cebdd788f99440468528a342afead63f9717323aa17679b661236c6fd'],
 'godpick_h228_market_competition.py': [None, '0f26619adeee759ab161886bcb08c6374e5d3a1f6be4c4ac81cb01448b897e0f'],
 'godpick_h229_net_edge.py': [None, 'f7f2c338fd8c85d30c3633bb146cbc89db89048944871258d1754798ff6ae6e0'],
 'godpick_h231_company_history.py': [None,
                                     'd9b17fdc0f904712a2b96daa1cf25e29f0ae23e6a7d292ba81f1ad603325117a',
                                     'b3de72318ee8b972a4834cc88e50e14c9c3bf34451f6b50ca0eaeddf6aa67c5b'],
 'godpick_h232_eps_comparability.py': [None, '02280d8dd343ec53844808fca5c3974f745c3122273dc08934ceb904489bd965'],
 'godpick_h233_auto_history.py': [None,
                                  '699ab8c8d09bdd70747bc0b7f2c2f30f705e68157a830a0d86217b436813caf4',
                                  'd0e43e39652488bf653ef038d98cb6d16b0ff7ffd29f5beb79ebbe59b12caa83'],
 'godpick_h234_batch_financial.py': [None],
 'godpick_h235_official_batch.py': [None],
 'godpick_h236_cloud_financial.py': [None],
 'godpick_h237_pipeline_acceptance.py': [None],
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
                             '4638d0424b22780502ca0de0f869317053d44ea8332335d5e2a9a9de6c9498e5',
                             '01839ef91b8b2ee3f7a14ed235c4a50dcfebb20b2dc8e267923e5b05f867a863',
                             'f4370e957a37436431162db07a155d479068386a606c811cb726ea0da321d23a',
                             'e707057d8312163c706e893dc0c347371ff3f4a1eef78c62476d96f5ccf38b4a',
                             'b14eb7f4807fd1622f1e8bc8ea0629cd16b3b0df5927fa800fff49ce2789f2a8',
                             '114f63af30d2c61ec6d0d9e75a93ad5c85b45abe6a10a16df6444b7d17033b9e',
                             '758a71787f5ff5d18a5e95ce8120e80b0156dfa69d41960697d47f287e427afa',
                             '36e223d97767e53d25c087eec9552f9e40d0afcea505025089598091feffe8ad',
                             '373eb52fc89fd883b88167b69ef45e5c851f9cf4c346a52d886e11c7eaf71a63',
                             '6e42075b9f47e6f34a6927b8297905cebb02224be5ba46a37c2715556aa18e53',
                             '379e413f7131213e03d6d06f5d4b84e2d7a1d2c7c7902184f5846960e0dc9182',
                             '241509230d54d15b913b5434b6d193e73c178328eeaba13314df521d0dd729fe'],
 'pages/33_超級AI股神決策總指揮中心.py': ['07d7706f10a97cde4350c7357aef9671fc320bfcab4233d92828b029fd0d7157',
                               'a8ca8bba6262b7a4ef2ac5d96a0862cf371d54692aea10d12699eb0db6e7ce80'],
 'pages/7_股神推薦.py': ['041059a53b4048b727cae3882ef0ef41d494b2027aadaff2c729d04d7644dde5',
                     '061c8809e6cb2a9fc14c691ee357d98225d9242d5d10c5159233c222343c6e31',
                     'b843a741b1758c818adf8e33e980b285c599f5ef6972c757345fbb9a1c65322e',
                     'f81e33fd1958fae90eeaac2d18107953ed926fd71d7529f52b480066b80cdf56',
                     '4d0385a6116f10c766dc9d64ee4d10750ba9e105be3f170f34137bfa90027be1',
                     '7ec4d5505699dc0c74c3866316ab8b84fd0f11456eafd97dec6f4bea6aa0153f',
                     '76bfda6712cd3a207048914afb6237cafc3c1f0172717124f313a37c18938b43'],
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
 'tests/test_h217_prospective_quality.py': ['47e4616238573e7f1b3a25dee9f67c358b721381aeb5595ee99b29b367f20da0',
                                            None,
                                            'e9a47096aa332bb9b663675715076684ea67a0401f736b001de83203342bb97c'],
 'tests/test_h218_evidence.py': ['537f168de78ac237e92488b63af6aa35cebc9af4aff93b3f440d3aaf974de1e0', None],
 'tests/test_h219_first_review.py': ['591ed36f126eab365b19c8efe423f2ffacc087629e6838e05fbf5a887010f840', None],
 'tests/test_h220_growth_rotation.py': ['45f3f2a31f470f300aefad77fb1bbb5a20b15399d52ae0c954756e6d9b08c3ae',
                                        None,
                                        'b2ed86fbd98e802c3a4c5d080551ef1d430e58246238f9005f65c4ae9e7d22bd'],
 'tests/test_h221_evidence_closure.py': [None, '2ef341bcadecd0172f5a18e6561b6932173626764ce467a603553cd1c196513e'],
 'tests/test_h222_financial_history.py': [None, '3a3b9374066ef45207b1fb5a63b7c2403e8d7a6ef03e29093f483d82900ef0fd'],
 'tests/test_h223_growth_scenarios.py': [None, '923f343618224d9a9a624bf5ea43551b7aa23be592c8971b59116298c5e9aa87'],
 'tests/test_h224_growth_resonance.py': [None, '015093b75c6732b8bf87b66b1d33437a3203e8065a9a44d31a49157a9f5f0ac2'],
 'tests/test_h225_integrated_quality.py': [None, 'd8a52327306b00786b2927ef04c722a0c9a4a6d6e82054b1231d7e4709431571'],
 'tests/test_h226_live_acceptance.py': [None, '76a98a5c07a6364b65455124086570ffb6afc75f33bd692bf53934a84fefdda6'],
 'tests/test_h227_strategy_validation.py': [None, '74a4972c86d9668b54b9ce98b12d86616ec2fe8b14c2bdb205eb5941882aea57'],
 'tests/test_h228_market_competition.py': [None, '0947bd3da427ac3cb27ce5b24ac9496171d57532492e454f88e1062653e122f0'],
 'tests/test_h229_net_edge.py': [None, 'e7015238c6ecd2ffecd464f211c76934e68ba5846ab1df029a321e89d609c4fb'],
 'tests/test_h230_quarter_evidence.py': [None, '01ad31c4cea12de84ae52dcba55137f34a12be0cf2755ce0c92a45d7555d1bab'],
 'tests/test_h231_company_history.py': [None, '7ed7ee1db041c19dd2090fd86d5c9bbf6f94283367442456063dcd8e1e95aca6'],
 'tests/test_h232_eps_comparability.py': [None, 'c15cb2c3d5137ff8fdcec36efa14e002e8b714ffee476782f1912255a4c702fd'],
 'tests/test_h233_auto_history.py': [None],
 'tests/test_h234_batch_financial.py': [None],
 'tests/test_h235_official_batch.py': [None],
 'tests/test_h236_cloud_financial.py': [None],
 'tests/test_h237_pipeline_acceptance.py': [None],
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
 'tools/h221_collect.py': [None, 'de7b054beed56d53321cccbc859e59b19789a7912bcf6338d2b9b7a7027a6e4a'],
 'tools/h222_history.py': [None,
                           '7bd6485126410af68d371707bbe73d99966946d944ebe5d7326f0fe765bcc733',
                           'd0d3d78eb0d472d1bd122653092033750be0f284e02a1813e190d97767929586',
                           'a2c2c23e5adaf12dfedd5ae044f9502867f8cd2776997bc6d0d698c7e0b9eba7',
                           '109041e9749878c8457f59f3caabbd5b121233cc45132f5c510692cd35825ce8'],
 'tools/h223_review.py': [None, '55ad730ee37068e0d4e4a6b90e122507e4226a408759d2c192ba8b534cddfe6d'],
 'tools/h224_review.py': [None, '6c220858e7e7d3fea52dc9144c3a3b54967750ef2a2dc378e05044c4ac2a3852'],
 'tools/h225_review.py': [None,
                          '3c916c05aa0bb37e7a1f34491dee56c9beebf5c9397bffdeb3e88c98960de121',
                          'e09d9e52d129b0b0157156c0177d2a87b0f0e168c260600544420e3a2c526650',
                          'a251fe504c59aa412e7866527f4623d6116704352aea84312763e58237d5e94f',
                          '0e255ec5f6de6185386166f656fa73cc60963de23241d01050010d3b6b396171',
                          '419842ebb728a3e4f279c00428b1bb274ef91a8666311257f5f300046fe237c3'],
 'tools/h236_cloud.py': [None],
 'tools/h237_acceptance.py': [None]}
DEPENDENCIES={'godpick_h123_flow_provenance.py': '9948d40b2796292ab63222fe8672e25c9e0b04796242bf05d9bd6ee1edd98174',
 'godpick_h197_strategy_review.py': '46d0b4e12463d5791ff7ccb3240e06b00c3608a3c4d46afe242c784d14910218',
 'godpick_h198_stage_review.py': '248db22227ffd8d82a9811f5ed97b12571720d1978ee3d3ab7c78dfdb2c27a50',
 'tools/h209_daily_backtest.py': '8a83d639762d28b373b3ffd8b5cb9d5af5a8947790ab054289026ae589487aac'}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h237-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H237_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or sha(p.read_bytes())!=want:errors.append('H237_DEPENDENCY_MISMATCH: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H237'
        elif (None if before is None else sha(before)) in base:state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h237_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
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
    if not todo:print('ALREADY_APPLIED_H237');return 0
    if not a.apply:print('READY_APPLY_H237');return 0
    print('APPLIED_H237 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
