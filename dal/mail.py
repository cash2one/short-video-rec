"""
# @file mail.py
# @Synopsis  send email
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-08
"""

from conf.env_config import EnvConfig
import commands
import socket
import getpass


class Mail(object):
    """
    # @Synopsis  send mail
    """

    @staticmethod
    def sendMail(receivers, title, content):
        """
        # @Synopsis  send mail
        #
        # @Args receivers list of receivers
        # @Args title
        # @Args content
        #
        # @Returns   succeeded or not
        """
        host_name = socket.gethostname()
        user_name = getpass.getuser()
        content = '{0}@{1}\t{2}'.format(user_name, host_name, content)

        bash_cmd = ' '.join(['echo "{0}"'.format(content),
            '| mail -s', '"{0}"'.format(title), ' '.join(receivers)])

        status, output = commands.getstatusoutput(bash_cmd)
        print 'Returned {0} : {1}'.format(status, bash_cmd)
        return status == 0

if __name__ == '__main__':
    Mail.sendMail(EnvConfig.MAIL_RECEIVERS, 'test mail', 'test content')

