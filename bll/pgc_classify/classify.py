"""
# @file classify.py
# @Synopsis  classify pgc videos into different categories
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-15
"""
import os
import json
from datetime import datetime

from conf.env_config import EnvConfig
from bll.data.data_source_conf import pgc_videos_table
from bll.data.data_source_conf import pgc_videos_increment
from bll.data.data_source_conf import test_tags_file


def classify(incremental=True):
    """
    # @Synopsis  classify pgc videos
    # @Args incremental
    # @Returns  None
    """
    source_table =  pgc_videos_increment if incremental else pgc_videos_table
    if EnvConfig.DEBUG:
        source_table.update('backup')
    else:
        source_table.update('online')
    videos = source_table.load()

    category_dict = {
            'car': 11,
            'funny': 5,
            'entertainment': 31,
            'music': 3,
            'game': 7
            }

    for category, category_id in category_dict.iteritems():
        category_videos = filter(lambda x: x['category_id'] == category_id, videos)
        title_set = set()
        dedupped_videos = []
        for video in category_videos:
            if video['title'] not in title_set:
                title_set.add(video['title'])
                dedupped_videos.append(video)

        def output_mapper(video):
            """
            # @Synopsis  map video to output format(with required key fields)
            # @Args video
            # @Returns   formated video
            """
            output_video = {
                    'title': video['title'],
                    'sub_title': video['sub_title'],
                    'url': video['url'],
                    'imgh_url': video['origin_image_link'],
                    'imgv_url': video['origin_image_link'],
                    'intro': '',
                    'duration': '{0:02d}:{1:02d}'.format(*divmod(video['duration'], 60)),
                    'play_num': video['play_cnt'],
                    'director': video['channel_title'],
                    'actor': video['channel_id'],
                    'weight': '1',
                    'update_time': datetime.fromtimestamp(video['insert_time']).strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    'sites': video['site']
                    }
            return output_video
        output_videos = map(output_mapper, dedupped_videos)
        output_dict = {
                'errno': 0,
                'msg': 'ok',
                'data': output_videos
                }

        output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'result',
                '{0}.json'.format(category))
        output_obj = open(output_path, 'w')
        output_obj.write(json.dumps(output_dict, ensure_ascii=False, indent=4).encode('utf8'))
        output_obj.close()
