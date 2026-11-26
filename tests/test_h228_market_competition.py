import io
from datetime import timedelta
import pytest
from openpyxl import load_workbook
import godpick_h228_market_competition as h
from test_h217_prospective_quality import sig,D,T,NOW,market

def fixture():
    m=market([D,T]);m.errors=[];m.today_confirmed=lambda d:True
    a={str(i):(100,30_000_000) for i in range(1000,1100)}
    z={c:(95+(int(c)-1000)*.1,30_000_000) for c in a}
    master={c:{'市場':'TWSE','股票名稱':c,'產業':'test'} for c in a}
    return m,a,z,master

def calc(ss,change=None):
    m,a,z,master=fixture()
    if change:change(m,a,z,master)
    r,d,e=h.competition('.',m,master,ss,D,T,NOW,reader=lambda root,mkt,day,now:(a if day==D else z,''))
    return next(x for x in r if x['市場']=='TWSE' and x['來源']=='H194' and x['研究群組']=='全研究'),d,e

def test_rank_lift():
    r,d,_=calc([sig('1099'),sig('1098')]);assert r['市場前十分位基準']==.1 and r['前十分位提升倍數']==10
    assert r['平均排名百分位']==pytest.approx(.99) and r['相對原價差']>0 and r['強勢榜提升倍數']==5
    assert len(d)==2 and d[0]['狀態']=='原價已核對／非結算'

def test_full_pool_no_advantage():
    r,_,_=calc([sig(str(i)) for i in range(1000,1100)]);assert r['平均排名百分位']==pytest.approx(.5) and r['前十分位提升倍數']==1 and r['強勢榜提升倍數']==1

def test_ties_actual_baseline():
    m,a,z,master=fixture();z={c:(100,30_000_000) for c in a};b=h.benchmark(master,'TWSE',a,z);assert len(b['top'])==100 and b['ready']
    r,_,_=calc([sig('1099')],lambda m,a,z,master:z.update({c:(100,30_000_000) for c in a}));assert r['前十分位提升倍數']==1 and r['強勢榜提升倍數'] is None and r['平均排名百分位']==.5

@pytest.mark.parametrize('mode',['missing','jump','base','calendar','future','coverage','market','empty'])
def test_guards(mode):
    ss=[sig('1099'),sig('1098')]
    def change(m,a,z,master):
        if mode=='missing':z.pop('1098')
        if mode=='jump':z['1098']=(140,30_000_000)
        if mode=='calendar':m.today_confirmed=lambda d:False
        if mode=='coverage':
            for i in range(1000,1006):z.pop(str(i))
    if mode=='base':ss[0]['base_close']=99
    if mode=='future':ss[0]['created_at']=T+'T20:00:00+08:00'
    if mode=='market':ss=[sig('1099',market='TPEX')]
    if mode=='empty':ss=[]
    r,_,_=calc(ss,change);assert r['研究原價平均'] is None and r['前十分位提升倍數'] is None and r['狀態']=='DATA_PENDING'

def test_duplicate_source():
    with pytest.raises(ValueError):calc([sig('1099'),sig('1099')])

def test_narrow_subset():
    rs=[{'市場':'TWSE','股票代號':str(i),'研究分層':h.FRONT[0],'官方收盤':100} for i in range(1000,1008)]
    rs[0]['反證／衝突']='warning';rs[1]['研究分層']='財報成長複核';rs[2]['官方收盤']=None
    rs.append({**rs[3],'市場':'TPEX'});out=h.narrow(rs);assert len(out)==6 and out[0]['股票代號']=='1003' and out[-1]['H228複核序']==1
    with pytest.raises(ValueError):h.narrow(rs+[rs[3]])

def test_narrow_never_promote():
    assert not h.narrow([{'市場':'TWSE','股票代號':'1000','研究分層':'原研究保留／未入精簡','官方收盤':100}])

def test_empty_run_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['batch_status']=='NO_FROZEN_BATCH' and r['weight_multiplier']==1 and r['trade_authority_changed'] is False
    assert not list(tmp_path.iterdir());w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10


def test_h209_without_tier():
    s=sig('1099',source='H209');s.pop('tier');_,d,_=calc([s]);assert d[0]['研究分級']=='H209獨立召回'

def test_incomplete_market_withholds_rank():
    r,d,_=calc([sig('1099')],lambda m,a,z,master:[z.pop(str(i)) for i in range(1000,1006)])
    assert d[0]['原價報酬'] is not None and d[0]['市場排名百分位'] is None and d[0]['前十分位'] is None
