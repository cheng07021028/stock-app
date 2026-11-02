from datetime import date
from godpick_h150_news_evidence import match_existing_news

DAY=date(2026,11,2)
def payload(**overrides):
 return {'match_type':'entity','title':'2330公司公告 - 官方消息','link':'https://news.google.com/rss/articles/someid',
         'published_at':'Mon, 02 Nov 2026 02:30:00 GMT',**overrides}

def test_valid_entity_news_is_labelled_unverified_not_trade():
 s=match_existing_news({'股票代號':'2330'},DAY,lambda r,max_items:[payload()])
 assert '關聯新聞待核' in s and '非交易因果' in s

def test_old_news_not_presented_as_current():
 assert not match_existing_news({'股票代號':'2330'},DAY,lambda r,max_items:[payload(published_at='Mon, 01 Jun 2026 02:30:00 GMT')])

def test_sector_news_not_pretended_entity_news():
 assert not match_existing_news({'股票代號':'2330'},DAY,lambda r,max_items:[payload(match_type='category')])

def test_fake_http_link_rejected():
 assert not match_existing_news({'股票代號':'2330'},DAY,lambda r,max_items:[payload(link='http://example.com')])

def test_missing_date_rejected():
 assert not match_existing_news({'股票代號':'2330'},DAY,lambda r,max_items:[payload(published_at='')])


def test_remote_cache_integrity_and_staleness():
 from datetime import datetime,timezone,timedelta
 from godpick_h150_news_evidence import verified_remote_news_payload
 from tools.h150_public_news import make_cache
 now=datetime(2026,11,2,13,tzinfo=timezone.utc)
 data=make_cache([{'title':'2330最新消息','link':'https://news.google.com/rss/a',
                   'published_at':now.isoformat(),'source':'新聞','query':'台股'}],generated_at=now)
 assert len(verified_remote_news_payload(data,now=now))==1
 assert verified_remote_news_payload({**data,'items_sha256':'0'*64},now=now)==[]
 assert verified_remote_news_payload(data,now=now+timedelta(days=5))==[]
