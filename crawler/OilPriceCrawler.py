import time
import pandas as pd
from datetime import datetime
import sqlite3
from .DBCrawler import DBCrawler

"""
    Description
    -----------
    유가 데이터를 수집하는 크롤러
"""

class OilPriceCrawler(DBCrawler):
    
    def __init__(self, conn):
        super().__init__(conn)
        
    def _create_table(self):
        """
            Description
            -----------
            테이블 생성
        """
        
        query = """
            CREATE TABLE IF NOT EXISTS OIL_PRICE (
                BASE_DATE TEXT,
                CODE TEXT,
                PRICE NUMBER,
                PRIMARY KEY (CODE, BASE_DATE)
            )
        """
        self.cur.execute(query)
        self.conn.commit()
        
    def set_code(self, codes):
        """
            Description
            -----------
            수집할 유가 코드들 설정
            
            Input
            -----
            code : OIL_CL(WTI), OIL_DU(두바이유), OIL_BRT(브렌트유)
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            opc = OilPriceCrawler(conn)
            opc.set_code(['OIL_CL', 'OIL_DU', 'OIL_BRT'])
            
        """
        
        self.codes = codes
        
    @staticmethod
    def get_oil_price(code):
        """
            Description
            -----------
            유가 데이터 수집 프로그램(from 네이버)

            Input
            -----
            code : OIL_CL(WTI), OIL_DU(두바이유), OIL_BRT(브렌트유)

            Output
            ------
            일별 유가(종가)

            Example
            -------
            oil_price_du = OilPriceCrawler.get_oil_price('OIL_DU')

        """

        delay = 0.01
        page = 1
        result = []
        start_time = datetime.now()

        # 수집
        print('[{}] 데이터 수집을 시작합니다. (code: {})'.format(start_time.strftime('%Y/%m/%d %H:%M:%S'), code))
        while(True):
            url = 'https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd={}&fdtc=2&page={}'.format(code, page)
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

        # 가공
        oil_price = pd.concat(result).reset_index(drop=True)
        oil_price.columns = ['BASE_DATE', 'PRICE', '전일대비', '등락율']
        oil_price['BASE_DATE'] = oil_price['BASE_DATE'].apply(lambda x: datetime.strptime(x, '%Y.%m.%d').strftime('%Y-%m-%d'))
        oil_price = oil_price[['BASE_DATE', 'PRICE']]
        oil_price.insert(1, 'CODE', code)

        end_time = datetime.now()
        print('[{}] 데이터 수집을 종료합니다. (code: {}, 수집시간: {}초, 데이터수: {:,}개)'.format(end_time.strftime('%Y/%m/%d %H:%M:%S'), code, (end_time-start_time).seconds, len(oil_price)))
        return oil_price
        
    def run(self):
        """
            Description
            -----------
            크롤러 실행
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            opc = OilPriceCrawler(conn)
            opc.set_code(['OIL_CL', 'OIL_DU', 'OIL_BRT'])
            opc.run()
        """
        
        for code in self.codes:
            oil_price = self.get_oil_price(code)
            oil_price.to_sql(name='OIL_PRICE', con=self.conn, if_exists='replace', index=False)