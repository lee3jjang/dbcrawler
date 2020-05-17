from abc import *

"""
    Description
    -----------
    크롤러 골격
"""

class DBCrawler(metaclass=ABCMeta):
    
    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()
        
    @abstractmethod
    def _create_table():
        pass
    
    @abstractmethod
    def run(self):
        pass