import time
import pandas as pd
from datetime import datetime
import sqlite3
from .DBCrawler import DBCrawler

"""
    Description
    -----------
    주가 데이터를 수집하는 크롤러
"""

class StockPriceCrawler(DBCrawler):
    
    def __init__(self, conn):
        super().__init__(conn)
        self.table_name = 'STOCK_PRICE'
        self._create_table()
        
    def _create_table(self):
        """
            Description
            -----------
            테이블 생성
        """
        
        query = """
            CREATE TABLE IF NOT EXISTS {table_name} (
                BASE_DATE TEXT,
                CODE TEXT,
                PRICE NUMBER,
                PRIMARY KEY (CODE, BASE_DATE)
            )
        """.format(table_name=self.table_name)
        self.cur.execute(query)
        self.conn.commit()
        
    def set_code(self, codes):
        """
            Description
            -----------
            수집할 주가 코드들 설정
            
            Input
            -----
            code : 005830(DB손해보험), 005930(삼성전자), 105560(KB금융)
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            spc = StockPriceCrawler(conn)
            spc.set_code(['005830', '005930', '105560'])
        """
        
        self.codes = codes
        
    @staticmethod
    def get_stock_price(code):
        """
            Description
            -----------
            주가 데이터 수집 프로그램(from 네이버)

            Input
            -----
            code : 005830(DB손해보험), 005930(삼성전자), 105560(KB금융)

            Output
            ------
            일별 유가(종가)

            Example
            -------
            stock_price_db = OilPriceCrawler.get_stock_price('005830')

        """

        delay = 0.01
        page = 1
        result = []
        start_time = datetime.now()

        # 수집
        print('[{}] 데이터 수집을 시작합니다. (code: {})'.format(start_time.strftime('%Y/%m/%d %H:%M:%S'), code))
        while(True):
            url = 'https://finance.naver.com//item/sise_day.nhn?code={}&page={}'.format(code, page)
            data = pd.read_html(url)[0].dropna()
            if page != 1:
                try:
                    if data.iloc[-1, 0] == result[-1].iloc[-1, 0]:
                        break
                except:
                    break
            result.append(data)
            page += 1
            time.sleep(delay)
            
        stock_price = pd.concat(result).reset_index(drop=True)
        stock_price.columns = ['BASE_DATE', 'PRICE', '전일비', '시가', '고가', '저가', '거래량']
        stock_price['BASE_DATE'] = stock_price['BASE_DATE'].apply(lambda x: datetime.strptime(x, '%Y.%m.%d').strftime('%Y-%m-%d'))
        stock_price = stock_price[['BASE_DATE', 'PRICE']]
        stock_price.insert(1, 'CODE', code)
        
        return stock_price
        
    def run(self):
        """
            Description
            -----------
            크롤러 실행
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            spc = StockPriceCrawler(conn)
            spc.set_code(['005830', '005930', '105560'])
            spc.run()
        """
        
        for code in self.codes:
            stock_price = self.get_stock_price(code)
            stock_price.to_sql(name=self.table_name, con=self.conn, if_exists='replace', index=False)