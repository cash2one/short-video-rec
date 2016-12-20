"""
# @file data_source_conf.py
# @Synopsis  contains configuration of data sources
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
import os
from datetime import date
from datetime import timedelta
from datetime import datetime
from conf.env_config import EnvConfig
from bll.data.table_file import TableFile
from bll.data.cached_mysql_table import CachedMySQLTable

__all__ = ['pgc_videos_table', 'pgc_videos_increment', 'test_tags_file']

def field_list_mapper(x):
    """
    # @Synopsis  map field list to requried format
    # @Args x
    # @Returns   list of dicts
    """
    if len(x) == 2:
        return {'field_name': x[0], 'data_type': x[1]}
    elif len(x) == 3:
        return {'field_name': x[0], 'column_name': x[1], 'data_type': x[2]}

field_list = [
        ['id', int],
        ['title', unicode]
        ]
field_list = map(field_list_mapper, field_list)
file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'source', 'test_tags.txt')
test_tags_file = TableFile(field_list, file_path)

field_list = [
        ['url', 'recommend_video.play_link', unicode],
        ['title', 'recommend_video.title', unicode],
        ['sub_title', 'recommend_video.sub_title', unicode],
        ['origin_image_link', 'recommend_video.image_link', unicode],
        ['duration', 'recommend_video.duration', int],
        ['play_cnt', 'recommend_video.play_num', int],
        ['site', 'recommend_video.site', unicode],
        ['insert_time', 'recommend_video.insert_time', int],
        ['channel_id', 'recommend_tag.id', int],
        ['channel_title', 'recommend_tag.title', unicode],
        ['channel_display_title', 'recommend_tag.display_title', unicode],
        ['channel_image_link', 'recommend_tag.imgurl', unicode],
        ['category_id', 'recommend_video.category_id', int]
        ]
field_list = map(field_list_mapper, field_list)
conditions = ['recommend_video.source_type=1', 'recommend_video.status=1',
        'recommend_video.id=album_video_rela.video_id',
        'album_video_rela.album_id=recommend_album.id',
        'recommend_album.tag_id=recommend_tag.id'
        ]
test_tags = test_tags_file.load()
test_tag_ids = map(lambda x: str(x['id']), test_tags)
test_tag_ids_str = ','.join(test_tag_ids)
conditions.append('recommend_tag.id NOT IN ({0})'.format(test_tag_ids_str))
condition_str = ' and '.join(conditions)

file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short',
        'source', 'pgc_candidates')
table_name = 'recommend_video, recommend_tag, recommend_album, album_video_rela'
pgc_videos_table = CachedMySQLTable(table_name = table_name, field_list=field_list,
        condition_str=condition_str, db_name='ChannelPC', file_path=file_path)

yesterday = date.today() - timedelta(1)
dt = datetime.combine(yesterday, datetime.min.time())
timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
conditions.append('recommend_video.insert_time>{0}'.format(timestamp))
condition_str = ' and '.join(conditions)
file_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'source',
        'pgc_candidates_increment')
pgc_videos_increment = CachedMySQLTable(table_name = table_name, field_list=field_list,
        condition_str=condition_str, db_name='ChannelPC', file_path=file_path)

