import time
import pandas as pd
from datetime import datetime
import sqlite3
from .DBCrawler import DBCrawler

"""
    Description
    -----------
    환율 데이터를 수집하는 크롤러
"""

class ExchangeRateCrawler(DBCrawler):
    
    def __init__(self, conn):
        super().__init__(conn)
        self.table_name = 'EXCHANGE_RATE'
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
                RATE NUMBER,
                PRIMARY KEY (CODE, BASE_DATE)
            )
        """.format(table_name=self.table_name)
        self.cur.execute(query)
        self.conn.commit()
        
    def set_code(self, codes):
        """
            Description
            -----------
            수집할 환율 코드들 설정
            
            Input
            -----
            code : FX_USDKRW(원/달러), FX_JPYKRW(원/엔), FX_CNYKRW(원/위안)
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            erc = ExchangeRateCrawler(conn)
            erc.set_code(['FX_USDKRW', 'FX_JPYKRW', 'FX_CNYKRW'])
        """
        
        self.codes = codes
        
    @staticmethod
    def get_exchange_rate(code):
        """
            Description
            -----------
            환율 데이터 수집 프로그램(from 네이버)

            Input
            -----
            code : FX_USDKRW(원/달러), FX_JPYKRW(원/엔), FX_CNYKRW(원/위안)

            Output
            ------
            일별 환율(매매기준율)

            Example
            -------
            exchange_rate_jpykrw = ExchangeRateCrawler.get_exchange_rate('FX_JPYKRW')

        """

        delay = 0.02
        page = 1
        result = []
        start_time = datetime.now()

        # 수집
        print('[{}] 데이터 수집을 시작합니다. (code: {})'.format(start_time.strftime('%Y/%m/%d %H:%M:%S'), code))
        while(True):
            url = 'https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd={}&page={}'.format(code, page)
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
            
        exchange_rate = pd.concat(result).reset_index(drop=True)
        exchange_rate.columns = ['BASE_DATE', 'RATE', '전일대비', '사실 때', '파실 때', '보내실 때', '받으실 때', 'T/C 사실 때', '외화수표 파실 때']
        exchange_rate['BASE_DATE'] = exchange_rate['BASE_DATE'].apply(lambda x: datetime.strptime(x, '%Y.%m.%d').strftime('%Y-%m-%d'))
        exchange_rate = exchange_rate[['BASE_DATE', 'RATE']]
        exchange_rate.insert(1, 'CODE', code)

        end_time = datetime.now()
        print('[{}] 데이터 수집을 종료합니다. (code: {}, 수집시간: {}초, 데이터수: {:,}개)'.format(end_time.strftime('%Y/%m/%d %H:%M:%S'), code, (end_time-start_time).seconds, len(exchange_rate)))
        
        return exchange_rate
        
    def run(self):
        """
            Description
            -----------
            크롤러 실행
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            erc = ExchangeRateCrawler(conn)
            erc.set_code(['FX_USDKRW', 'FX_JPYKRW', 'FX_CNYKRW'])
            erc.run()
        """
        
        for code in self.codes:
            exchange_rate = self.get_exchange_rate(code)
            exchange_rate.to_sql(name=self.table_name, con=self.conn, if_exists='replace', index=False)