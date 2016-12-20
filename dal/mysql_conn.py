"""
# @file mysql_dal.py
# @Synopsis  mysql dal, connet to ns_video db and final db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""

import os
import logging
import MySQLdb
import ConfigParser
from conf.env_config import EnvConfig
from dal.get_instance_by_service import getInstanceByService

logger = logging.getLogger(EnvConfig.LOG_NAME)

class MySQLConn(object):
    """
    # @Synopsis  mysql dal
    """
    CONF_PATH_DICT = {
            'online': os.path.join(EnvConfig.CONF_PATH, 'mysql', 'online.cfg'),
            'backup': os.path.join(EnvConfig.CONF_PATH, 'mysql', 'backup.cfg'),
            'test': os.path.join(EnvConfig.CONF_PATH, 'mysql', 'test.cfg'),
            }

    def __init__(self, db_name, db_type):
        conf_path = MySQLConn.CONF_PATH_DICT[db_type]
        config = ConfigParser.RawConfigParser()
        config.read(conf_path)
        if db_type == 'backup':
            bns = config.get(db_name, 'bns')
            host, port = getInstanceByService(bns)
        else:
            host = config.get(db_name, 'host')
            port = config.getint(db_name, 'port')

        user = config.get(db_name, 'user')
        passwd = config.get(db_name, 'passwd')
        db = config.get(db_name, 'db')
        self.conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db,
                use_unicode=True)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def insert(self, table_name, rows):
        """
        # @Synopsis  insert rows to database
        # @Args table_name
        # @Args rows
        # @Returns   number of successfully inserted rows
        """
        if len(rows) == 0:
            return 0
        column_name_list = rows[0].keys()

        def value_mapper(row):
            """
            # @Synopsis  map dict to value list, order of which consist with column
            # name list
            # @Args row
            # @Returns   value list
            """
            values = []
            for column_name in column_name_list:
                values.append(row[column_name])
            return values
        values = map(value_mapper, rows)
        sql = 'insert into {0}({1}) values({2})'.format(table_name, ','.join(column_name_list),
                ','.join(['%s'] * len(column_name_list)))
        logger.debug(sql)
        n = self.cursor.executemany(sql, values)
        self.conn.commit()
        return n

    def select(self, table_name, field_list, condition_str):
        """
        # @Synopsis  select from mysqldb
        #
        # @Args table_name
        # @Args field_list
        # @Args condition_str
        #
        # @Returns   rows
        """
        field_col_str_list = map(lambda x: '{column_name} as {field_name}'.format(**x), field_list)
        field_col_str = ','.join(field_col_str_list)
        sql_cmd = 'select {0} from {1} where {2}'.format(field_col_str, table_name, condition_str)
        logger.debug(sql_cmd)
        self.cursor.execute(sql_cmd)
        ret = self.cursor.fetchall()
        return ret


