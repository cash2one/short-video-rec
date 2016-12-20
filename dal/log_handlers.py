"""
# @file log_handlers.py
# @Synopsis  log handler, send sms and/or mail
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-06
"""
import logging
from sms import SMS
from mail import Mail

class SMSHandler(logging.Handler):
    """
    # @Synopsis  inherit logging.Handler to make a customized log handler to send
    # email, because I don't know how the provided logging.handlers.SMTPHandler
    # works with the linux system mail service
    """

    def __init__(self, receivers):
        super(self.__class__, self).__init__()
        self.receivers = receivers

    def emit(self, record):
        """
        # @Synopsis  override emit method, to deal with the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        SMS.sendMessage(self.receivers, msg)


class MailHandler(logging.Handler):
    """
    # @Synopsis  customized handler, to email critical log
    """

    def __init__(self, receivers):
        super(self.__class__, self).__init__()
        self.receivers = receivers

    def emit(self, record):
        """
        # @Synopsis  override logging.Handler emit method, the action when receive
        # the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        Mail.sendMail(self.receivers, 'PROGRAM ALARM', msg)
