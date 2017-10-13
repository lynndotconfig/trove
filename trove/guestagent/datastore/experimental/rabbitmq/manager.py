from oslo_log import log as logging

from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rb_instance
from trove.common.notification import EndNotification
from trove.common import utils
from trove.guestagent import backup
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.rabbitmq import service
from trove.guestagent.datastore import manager
from trove.guestagent import volume

LOG = logging.getLogger(__name__)


class Manager(manager.Manager):
    """
    This is the Rabbitmq Manager class. It is dynamically loaded
    based off of the service_type of the trove instance.
    """

    def __init__(self):
        super(Manager, self).__init__('rabbitmq')
        self._app = service.RabbitmqApp()

    @property
    def status(self):
        return self._app.status

    @property
    def configuration_manager(self):
        return self._app.configuration_manager

    def restart(self, context):
        """
        Restart this rabbitmq instance.
        This method is called when the guest agent
        gets a restart message from the taskmanager.
        """
        LOG.debug("Restart called.")
        self._app.restart()

    def start_db_with_conf_changes(self, context, config_contents):
        """
        Start this rabbitmq instance with new conf changes.
        """
        LOG.debug("Start DB with conf changes called.")
        self._app.start_db_with_conf_changes(config_contents)

    def stop_db(self, context, do_not_start_on_reboot=False):
        """
        Stop this rabbitmq instance.
        This method is called when the guest agent
        gets a stop message from the taskmanager.
        """
        LOG.debug("Stop DB called.")
        self._app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_info(self):
        return self._app.admin.get_info()

    def get_nodes(self):
        return self._app.admin.get_nodes()

    def get_top(self):
        return self._app.admin.get_top()

    def get_whoami(self):
        return self._app.admin.get_whoami()

    def list_connections(self):
        return self._app.admin.list_connections()

    def list_exchanges(self, virtual_host='/', show_all=False):
        return self._app.admin.list_exchanges(
            virtual_host=virtual_host, show_all=show_all)

    def list_queues(self, virtual_host='/', show_all=False):
        return self._app.admin.list_queues(
            virtual_host=virtual_host, show_all=show_all)

    def list_users(self):
        return self._app.admin.list_users()

    def list_virtual_hosts(self):
        return self._app.admin.list_virtual_hosts()
