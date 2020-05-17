import sqlite3
from crawler.OilPriceCrawler import OilPriceCrawler

if __name__ == '__main__':
    conn = sqlite3.connect('external_data.db')
    opc = OilPriceCrawler(conn)
    opc.set_code(['OIL_CL', 'OIL_DU', 'OIL_BRT'])
    opc.run()