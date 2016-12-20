"""
# @file update_hot_new.py
# @Synopsis  selet hotest an newest video and save to mola
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-21
"""
import sys
sys.path.append('..')
from conf.init_logger import InitLogger
from bll.pgc_hot_new.update import update_hot_new_videos

if __name__ == '__main__':
    InitLogger()
    update_hot_new_videos(50)
