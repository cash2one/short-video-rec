"""
# @Synopsis  select video with highest play cnt and with latest insert time,
# and save to mola
"""
import os
import heapq
import logging
import json
from conf.env_config import EnvConfig
from bll.data.data_source_conf import pgc_videos_table
from dal.mola import Mola

logger = logging.getLogger(EnvConfig.LOG_NAME)

def update_hot_new_videos(cnt):
    """
    # @Synopsis  select video with highest play cnt and with latest insert time,
    # and save to mola
    # @Args cnt
    # @Returns   None
    """
    MOLA_KEY_PREFIX = 'guming_pgc_table:'
    if EnvConfig.DEBUG:
        pgc_videos_table.update('backup')
    else:
        pgc_videos_table.update('online')
    videos = pgc_videos_table.load()

    if len(videos) < cnt:
        logger.critical('Total pgc video number is less than hot cnt')
        return False

    hot_videos = heapq.nlargest(cnt, videos, key=lambda x: x['play_cnt'])
    hot_video_urls = map(lambda x: x['url'], hot_videos)
    hot_video_urls_str = json.dumps(hot_video_urls, ensure_ascii=False)
    new_videos = heapq.nlargest(cnt, videos, key=lambda x: x['insert_time'])
    new_video_urls = map(lambda x: x['url'], new_videos)
    new_video_urls_str = json.dumps(new_video_urls, ensure_ascii=False)

    output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'result', 'hot_new_videos')
    output_obj = open(output_path, 'w')
    output_obj.write(u'{0}\t{1}\n{2}\t{3}\n'.format('hot_videos', hot_video_urls_str, 'new_videos',
        new_video_urls_str).encode('utf8'))
    output_obj.close()
    Mola.updateDb(MOLA_KEY_PREFIX, output_path)

    def output_mapper(video):
        """
        # @Synopsis  map video to output format(with required key fields)
        # @Args video
        # @Returns   formated video
        """
        output_video = {
                'link': video['url'],
                'title': video['title'],
                'image_link': video['origin_image_link'],
                'duration': video['duration'],
                'channel_id': video['channel_id'],
                'channel_title': video['channel_title'],
                'channel_display_title': video['channel_display_title'],
                'channel_image_link': video['channel_image_link'],
                }
        return output_video
    output_videos = map(output_mapper, hot_videos + new_videos)


    output_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, 'short', 'result',
            'hot_new_video_meta')
    output_obj = open(output_path, 'w')
    for video in output_videos:
        meta_json = json.dumps(video, ensure_ascii=False)
        output_obj.write(u'meta:{0}\t{1}\n'.format(video['link'], meta_json).encode('utf8'))
    output_obj.close()
    Mola.updateDb(MOLA_KEY_PREFIX, output_path)

