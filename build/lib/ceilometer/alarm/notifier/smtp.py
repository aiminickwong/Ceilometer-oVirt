#
# Copyright 2015-2016 China Mobile
#
# Author: Luo Gangyi <luogangyi@Chinamobile.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Smtp alarm notifier."""

import eventlet
from oslo.config import cfg
import smtplib

from ceilometer.alarm import notifier
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)

REST_NOTIFIER_OPTS = [
    cfg.StrOpt('email_notifier_smtp_address',
               default='localhost',
               help='SMTP address for email notifier.'
               ),
    cfg.IntOpt('email_notifier_smtp_port',
               default=25,
               help='SMTP address for email notifier.'
               ),
    cfg.StrOpt('email_notifier_smtp_username',
               default='',
               help='SMTP username for email notifier.'
               ),
    cfg.StrOpt('email_notifier_smtp_password',
                default='',
                help='SMTP password for email notifier.'
                ),
]

cfg.CONF.register_opts(REST_NOTIFIER_OPTS, group="alarm")


class SmtpAlarmNotifier(notifier.AlarmNotifier):
    """Rest alarm notifier."""

    @staticmethod
    def notify(action, alarm_id, previous, current, reason, reason_data):

        LOG.info(_(
            "Notifying alarm %(alarm_id)s from %(previous)s "
            "to %(current)s with action %(action)s because "
            "%(reason)s.") %
            ({'alarm_id': alarm_id, 'previous': previous,
              'current': current, 'action': action,
              'reason': reason}))

        smtp_address = cfg.CONF.alarm.email_notifier_smtp_address
        smtp_port = cfg.CONF.alarm.email_notifier_smtp_port
        smtp_username = cfg.CONF.alarm.email_notifier_smtp_username
        smtp_password = cfg.CONF.alarm.email_notifier_smtp_password
        target_address = action.geturl()
        mail_from = "From: %s\n" % smtp_username
        mail_to = "To: %s\n" % target_address
        subject = "Subject: %s\n\n" % 'Alarm From BCEC!'

        content = "Notifying alarm %(alarm_id)s from %(previous)s " \
                  "to %(current)s with action %(action)s because "  \
                  "%(reason)s." % \
                  ({'alarm_id': alarm_id, 'previous': previous,
                    'current': current, 'action': action,
                    'reason': reason,
                    })
        message = mail_from + mail_to + subject + content
        eventlet.spawn_n(smtp_address, smtp_address, smtp_port,
                         smtp_username, smtp_password, target_address,
                         message)

    @staticmethod
    def _send_mail(smtp_address, smtp_port, smtp_username, smtp_password,
                    target_address, message):
        try:
            smtp = smtplib.SMTP()
            smtp.connect(smtp_address, smtp_port)
            smtp.login(smtp_username, smtp_password)
            smtp.sendmail(smtp_username, target_address, message)
            smtp.quit()
        except Exception, e:
            LOG.error("Error in Sending alarm email to %s" % target_address)
