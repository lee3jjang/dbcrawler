import urllib.request as req
from urllib.parse import urlparse, urlencode
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from .DBCrawler import DBCrawler

class PnoCrawler(DBCrawler):

    def __init__(self, conn):
        super().__init__(conn)
        self.table_name = 'CETIZEN_PNO'
        self._create_table()

        url = 'https://price.cetizen.com/'
        res = req.Request(url)
        html = req.urlopen(res).read().decode('cp949')
        self.soup = BeautifulSoup(html, 'html.parser')
        self.wireless = {
            'wireless_1[]': 'S',
            'wireless_2[]': 'K',
            'wireless_3[]': 'L',
            'wireless_7[]': 'SELF',
            'wireless_1,2[]': 'S,K',
            'wireless_1,3[]': 'S,L',
            'wireless_2,3[]': 'K,L',
            'wireless_1,2,3[]': 'S,K,L',
            'wireless_1,2,3,7[]': 'S,K,L,SELF',
            'wireless_9[]': '해당없음',
            'wireless_0[]': '해외'
        }

    def _create_table(self):
        """
            Description
            -----------
            테이블 생성
        """
        
        query = """
            CREATE TABLE IF NOT EXISTS {table_name} (
            	PNO TEXT PRIMARY KEY,	
                MODEL TEXT,
                WIRELESS TEXT
            )
        """.format(table_name=self.table_name)
        self.cur.execute(query)
        self.conn.commit()
    
    def _get_info(self, tag):
        tag2 = tag.find_all('li', {'style': re.compile('^float:left')})
        pno = urlparse(tag2[0].a['href']).query.split('&')[1].split('=')[1]
        name = tag2[0].text
        model = tag2[1].text
        price = tag2[2].text
        return pno, name, model, price

    def _get_info_wireless(self, wireless):
        # id=make_0 인 애들 말고 하나씩 더 있어서 2개씩 중복됨(drop_duplicates 해야 함)
        tag = self.soup.find_all('div', {'name': wireless[0]})
        result = []
        for i in range(len(tag)):
            result.append([wireless[1], *self._get_info(tag[i])])
        pno = pd.DataFrame(result, columns=['WIRELESS', 'PNO', 'MODEL', '중고시세', '증감'])
        pno = pno[['PNO', 'MODEL', 'WIRELESS']].drop_duplicates().reset_index(drop=True)
        return pno

    def run(self):
        result = []
        for wl in self.wireless.items():
            result.append(self._get_info_wireless(wl))
        pno = pd.concat(result)
        pno.to_sql(name=self.table_name, con=self.conn, if_exists='replace', index=False)


# class ReleasePriceCrawler(CetizenCrawler):

#     def __init__(self, pno):
#         self.tableName = '출고가정보'
#         super().__init__()
#         self.pno = pno

#     def crawling(self, save=True):
#         rst_list = []
#         for pl in self.pno:
#             params = {'act': 'factory_price', 'q': 'info', 'pno': pl}
#             url = 'https://market.cetizen.com/market.php'
#             url_params = '{}?{}'.format(url, urlencode(params))
#             html = req.urlopen(url_params).read().decode('cp949')
#             soup = BeautifulSoup(html, 'html.parser')
#             txt = soup.find_all('script', {'type': "text/javascript"})[18].text
#             start = txt.find('[')
#             end = txt.find(']')
#             txt = txt[start:end+1].replace('\r\n\t','')\
#                 .replace('date', '"date"').replace('value', '"value"').replace(': }', ':None}')
#             data = eval(txt)
#             for dt in data:
#                 rst_list.append((pl, dt['date'], dt['value']))
#         df = self._add_info(pd.DataFrame(rst_list, columns=['pno', 'date', 'value']))
#         if save:
#             self._save(df)
#         return df


# class UsedPriceCrawler(CetizenCrawler):

#     def __init__(self, pno):
#         self.tableName = '중고가정보'
#         super().__init__()
#         self.pno = pno

#     def crawling(self, save=True):
#         rst_list = []
#         for pl in self.pno:
#             params = {'q': 'info', 'pno': pl}
#             url = 'https://market.cetizen.com/market.php'
#             url_params = '{}?{}'.format(url, urlencode(params))
#             html = req.urlopen(url_params).read().decode('cp949')
#             soup = BeautifulSoup(html, 'html.parser')
#             txt = soup.find_all('script', {'type': "text/javascript"})[18].text
#             start = txt.find('[')
#             end = txt.find(']')
#             txt = txt[start:end+1].replace('\r\n\t', '')\
#                 .replace('date', '"date"').replace('mid', '"mid"').replace('high', '"high"').replace('low', '"low"')
#             data = eval(txt)
#             for dt in data:
#                 rst_list.append((pl, dt['date'], dt['low'], dt['mid'], dt['high']))
#         df = self._add_info(pd.DataFrame(rst_list, columns=['pno', 'date', 'low', 'mid', 'high']))
#         if save:
#             self._save(df)
#         return df



