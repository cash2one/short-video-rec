"""
# @file update_db.py
# @Synopsis  insert/update similar work result to mysql db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-22
"""
import sys
import os
sys.path.append('../..')
import time
import logging
import json
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.data.data_source_conf import lvideo_related_svideos_dict
from dal.mysql_conn import MySQLConn
from dal.image_extractor import ImageExtractor

logger = logging.getLogger(EnvConfig.LOG_NAME)

def updateLVideoRelatedSvideo(work_type):
    """
    # @Synopsis  update mysql db similar_works table with calculated similar
    # work results
    # @Args works_type
    # @Returns   succeeded or not(to be done)
    """
    insert_video_list = []
    if EnvConfig.DEBUG:
        lvideo_related_svideos_dict[work_type].update('test')
    else:
        lvideo_related_svideos_dict[work_type].update('online')
    exist_videos = lvideo_related_svideos_dict[work_type].load()
    exist_relation_list = map(lambda x: ((x['work_id'], x['url']), x), exist_videos)
    exist_relation_dict = dict(exist_relation_list)

    rec_list_file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH,
            work_type, 'result', 'rec_list')
    rec_list_obj = open(rec_list_file_path)
    for line in rec_list_obj:
        line = line.decode('utf8')
        work_id, json_str = line.strip('\n').split('\t')
        short_videos = json.loads(json_str)
        for video in short_videos:
            if (work_id, video['url']) not in exist_relation_dict:
                insert_video = dict(video)
                insert_video['work_id'] = work_id
                insert_video_list.append(insert_video)
    # image_extractor = ImageExtractor()
    # insert_video_list = image_extractor.extract(insert_video_list)
    logger.debug('{0} lines to insert'.format(len(insert_video_list)))

    def insert_mapper(video):
        """
        # @Synopsis  map video to database required k-v format
        # @Args video
        # @Returns   formatted dict
        """
        out_dict = {
                'work_id': video['work_id'],
                'title': video['title'],
                'link': video['url'],
                'origin_image_link': video['origin_image_link'],
                'image_link': video['origin_image_link'],
                'duration': video['duration'],
                'display_order': video['display_order'],
                'is_pgc': video['is_pgc'],
                'is_online': 1,
                'put_on_top': 0,
                'channel_title': video['channel_title'],
                'channel_display_title': video['channel_display_title'],
                'channel_image_link': video['channel_image_link']
                }
        return out_dict
    insert_video_list = map(insert_mapper, insert_video_list)
    table_name = '{0}_related_svideo'.format(work_type)

    if EnvConfig.DEBUG:
        conn = MySQLConn('ChannelPC', 'test')
    else:
        conn = MysqlConn('ChnnelPC', 'online')

    n = conn.insert(table_name, insert_video_list)
    logger.debug('{0} lines inserted'.format(n))
    return True
