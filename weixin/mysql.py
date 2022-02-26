import pymysql
from weixin.config import *


class MySQL():
    def __init__(self, host=MYSQL_HOST, username=MYSQL_USER, password=MYSQL_PASSWORD, port=MYSQL_PORT,
                 database=MYSQL_DATABASE):
        """
        MySQL初始化
        :param host:
        :param username:
        :param password:
        :param port:
        :param database:
        """
        try:
            #self.db = pymysql.connect(host, username, password, database, charset='utf8', port=port)
            print("host:{}, user:{}, password:{}, db:{}".format(host, username, password, database))
            self.db = pymysql.connect(host=host, user=username, password=password, db=database )
            self.cursor = self.db.cursor()
        except pymysql.MySQLError as e:
            print(e.args)
    
    def insert(self, table, data):
        """
        插入数据
        :param table:
        :param data:
        :return:
        """
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql_query = 'insert into %s (%s) values (%s)' % (table, keys, values)
        try:
            self.cursor.execute(sql_query, tuple(data.values()))
            self.db.commit()
        except pymysql.MySQLError as e:
            print(e.args)
            self.db.rollback()
