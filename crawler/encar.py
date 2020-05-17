import os
import time
from glob import glob
import pandas as pd
from random import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from .DBCrawler import DBCrawler
from datetime import datetime

class UsedCarPriceCrawler(DBCrawler):
    def __init__(self, conn):
        super().__init__(conn)
        self.table_name = 'ENCAR_USED_CAR_PRICE'
        self._create_table()
        self.url_map = {
            'benz': 'http://www.encar.com/fc/fc_carsearchlist.do?carType=for&searchType=model&TG.R=B#!%7B%22action%22%3A%22(And.Hidden.N._.(C.CarType.N._.Manufacturer.%EB%B2%A4%EC%B8%A0.))%22%2C%22toggle%22%3A%7B%7D%2C%22layer%22%3A%22%22%2C%22sort%22%3A%22ModifiedDate%22%2C%22page%22%3A1%2C%22limit%22%3A20%7D',
            'ev': 'http://www.encar.com/ev/ev_carsearchlist.do?carType=ev&searchType=model&TG.R=D#!',
            'bmw': 'http://www.encar.com/fc/fc_carsearchlist.do?carType=for&searchType=model&TG.R=B#!%7B%22action%22%3A%22(And.Hidden.N._.(C.CarType.N._.Manufacturer.BMW.))%22%2C%22toggle%22%3A%7B%7D%2C%22layer%22%3A%22%22%2C%22sort%22%3A%22ModifiedDate%22%2C%22page%22%3A1%2C%22limit%22%3A20%7D',
        }
        self.page_max = 3
        self.driver = webdriver.Chrome('chromedriver')

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
                NAME1 TEXT,
                NAME2 TEXT,
                NAME3 TEXT,
                NAME4 TEXT,
                YER TEXT,
                FUE TEXT,
                LOC TEXT,
                INS TEXT,
                ASS TEXT,
                PRC TEXT,
                LINK TEXT
            )
        """.format(table_name=self.table_name)
        self.cur.execute(query)
        self.conn.commit()

    def set_code(self, codes):
        self.codes = codes

    def run(self):
        """
            Description
            -----------
            크롤러 실행
            
            Example
            -------
            conn = sqlite3.connect('external_data.db')
            ucpc = UsedCarPriceCrawler(conn)
            ucpc.set_code(['benz', 'ev', 'bmw'])
            ucpc.run()
        """

        for code in self.codes:
            self._get_source(code)
            df = self._parse(code)
            df.to_sql(name=self.table_name, con=self.conn, if_exists='replace', index=False)

    def _get_source(self, code):
        '''
            Description:
                주어진 url로 접근 → 일반등록 차량에 있는 데이터를 html 형태로 수집
                
            Input:
                code - 저장할 폴더명
                
            Output:
                temp 폴더에 결과 저장
            
            Example:
                get_source('benz')
        '''
        
        # 폴더 생성
        if not any([s == 'temp' for s in os.listdir('.')]): os.mkdir('temp')
        if not any([s == code for s in os.listdir('temp')]): os.mkdir('temp/{}'.format(code))
        
        # 접속하기
        print('({}) 첫 페이지에 접근합니다.'.format(code))
        self.driver.get(self.url_map[code])

        # [20개씩 보기] → [50개씩 보기]로 변환
        viewer = Select(self.driver.find_element_by_css_selector('select#pagerow'))
        time.sleep(1)
        viewer.select_by_value('50')
        time.sleep(10)

        # 수집하기
        page = 1
        print('수집을 시작합니다.')
        while(True):
            if(page > self.page_max):
                print('수집을 종료합니다. (페이지 지정치 도달)')
                break
            with open('temp/{}/carlist_{}_{:04d}.html'.format(code, code, page), 'w', -1, encoding='utf-8') as f:
                soup = BeautifulSoup(self.driver.find_element_by_xpath('//tbody[@id="sr_normal"]/ancestor::table').get_attribute('outerHTML'), 'lxml')
                html = str(soup)
                f.write(html)
            try:
                self.driver.find_element_by_css_selector('div#pagination').find_element_by_xpath('//a[@data-page="{}"]'.format(page+1)).click()
            except NoSuchElementException:
                print('수집을 종료합니다. (NoSuchElementException)')
                break
            page += 1
            time.sleep(1+2*random())
        
    def _parse(self, code):
        '''
            Description:
                get_source를 통해 수집된 html파일에서 데이터를 추출하여 xlsx 형태로 저장
                
            Input:
                code - get_source를 통해 저장한 폴더명
                
            Output:
                temp 폴더에 결과 저장
        '''
        
        print('({}) 파싱을 시작합니다.'.format(code))
        result = []
        files = glob('temp/{}/*.html'.format(code))
        for file in files:
            with open(file, 'r', -1, encoding='utf-8') as f:
                html = f.read()
            soup = BeautifulSoup(html, 'lxml')
            carlist_batch = soup.select('tr')[1:]
            result_batch = []
            for car in carlist_batch:
                name1 = car.select_one('span.cls > strong').text
                name2 = car.select_one('span.cls > em').text
                name3 = car.select_one('span.dtl > strong').text
                name4 = car.select_one('span.dtl > em').text
                yer = car.select_one('span.yer').text
                km = car.select_one('span.km').text
                fue = car.select_one('span.fue').text
                loc = car.select_one('span.loc').text
                ins = '' if car.select_one('span.ins') == None else car.select_one('span.ins').text
                ass = '' if car.select_one('span.ass') == None else car.select_one('span.ass').text
                prc = car.select_one('td.prc_hs').text
                link = car.select_one('a').attrs['href']
                result_batch.append((name1, name2, name3, name4, yer, km, fue, loc, ins, ass, prc, link))
            column_name = ['NAME1', 'NAME2', 'NAME3', 'NAME4', 'YER', 'KM', 'FUE', 'LOC', 'INS', 'ASS', 'PRC', 'LINK']
            df = pd.DataFrame(result_batch, columns=column_name)
            result.append(df)
        df = pd.concat(result).reset_index(drop=True)
        df.insert(0, 'BASE_DATE', datetime.now().strftime('%Y-%m-%d'))
        df.insert(1, 'CODE', code)
        df['LINK'] = 'http://www.encar.com' + df['LINK']
        print('파싱이 종료되었습니다.')

        return df