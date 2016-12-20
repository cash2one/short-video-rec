"""
# @file classify.py
# @Synopsis  classify pgc videos
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-15
"""
import sys
sys.path.append('..')
import logging
from conf.init_logger import InitLogger
from conf.env_config import EnvConfig
from bll.data.data_source_conf import pgc_videos_table
from bll.pgc_classify.classify import classify


if __name__ == '__main__':
    InitLogger()
    classify(incremental=True)
