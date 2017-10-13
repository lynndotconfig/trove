"""
Determines operating system version and OS dependent commands.
"""

from trove.guestagent.common.operating_system import get_os


RABBITMQ_OWNER = 'rabbitmq'
RABBITMQ_CONFIG = '/etc/rabbitmq/rabbitmq.conf'
RABBITMQ_PID_FILE = '/var/run/rabbitmq/rabbitmq-server.pid'
RABBITMQ_LOG_FILE = '/var/log/rabbitmq/rabbit.log'
RABBITMQ_CONF_DIR = '/etc/rabbitmq'
RABBITMQ_DATA_DIR = '/var/lib/rabbitmq'
RABBITMQ_PORT = '5567'
RABBITMQ_INIT = '/etc/init/rabbitmq-server.conf'
RABBITMQ_PACKAGE = ''
SERVICE_CANDIDATES = ['rabbitmq-server', 'rabbitmqctl']
