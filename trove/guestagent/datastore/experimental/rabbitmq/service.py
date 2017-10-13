import os

from amqpstorm.management import api
from amqpstorm.management.exception import ApiError

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rb_instance
from trove.common.stream_codecs import PropertiesCodec, StringConverter
from trove.common import utils as utils
from trove.guestagent.common.configuration import ConfigurationManager
from trove.guestagent.common.configuration import OneFileOverrideStrategy
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore import service
from trove.guestagent import pkg

from trove.guestagent.datastore.experimental.rabbitmq import system

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

class RabbitmqStatus(service.BaseDbStatus):
    """
    Handles all the status updating for the rabbitmq guest agent.
    """

    def __init__(self, client):
        super(RabbitmqStatus, self).__init__()
        self.__client = client

    def set_client(self, client):
        """set client."""
        self.__client = client

    def _get_actual_db_status(self):
        try:
            if self.__client.ping():
                return rb_instance.ServiceStatuses.RUNNING
        except ApiError:
            LOG.exception(_("Error getting Rabbitmq status."))

        return rb_instance.ServiceStatuses.CRASHED
    
    def cleanup_stalled_db_services(self):
        utils.execute_with_timeout(
            'pkill', '-9',
            'rabbitmq-server',
            run_as_root=True,
            root_helper='sudo'
        )

class RabbitmqApp(object):
    """
    Handles installation and configuration of the rabbitmq
    in trove.
    """

    def __init__(self, state_change_wait_time=None):
        """
        Set default status and state_change_wait_time.
        """
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time

        revision_dir = guestagent_utils.build_file_path(
            os.path.dirname(system.RABBITMQ_CONFIG),
            ConfigurationManager.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR
        )
        config_value_mappings = {'yes': True, 'no': False, "''": None}
        self._value_converter = StringConverter(config_value_mappings)
        self.configuration_manager = ConfigurationManager(
            system.RABBITMQ_CONFIG,
            system.RABBITMQ_OWNER, system.RABBITMQ_OWNER,
            PropertiesCodec(
                unpack_singletons=False,
                string_mappings=config_value_mappings
            ), requires_root=True,
            override_strategy=OneFileOverrideStrategy(revision_dir)
        )

        self.admin = self._build_admin_client()
        self.status = RabbitmqStatus(self.admin)

    def _build_admin_client(self):
        api_url = self.get_configuration_property('api_url')
        username = self.get_configuration_property('username')
        password = self.get_configuration_property('requirepass')

        return RabbitmqAdmin(
            api_url=api_url, username=username, password=password)

    def install_if_needed(self, packages):
        """
        Install rabbitmq if needed to  nothing if it is already installed.
        """
        pass

    def start_db_with_conf_changes(self, config_contents):
        LOG.info(_('Starting rabbitmq with conf changes.'))
        if self.status.is_running:
            msg = 'Cannot start_db_with_conf_changes because status is %s.'
            LOG.debug(msg, self.status)
            raise RuntimeError(msg % self.status)
        LOG.info(_("Initiating config."))
        self.configuration_manager.save_configuration(config_contents)
        # The configuration template has to be updated with
        # guestagent-controlled settings.
        self.apply_initial_guestagent_configuration()
        self.start_db(True)

    def start_db(self, enable_on_boot=True, update_db=False):
        self.status.start_db_service(
            system.SERVICE_CANDIDATES, CONF.state_change_wait_time,
            enable_on_boot=enable_on_boot, update_db=update_db)

    def apply_initial_guestagent_configuration(self):
        """Update guestagent-controlled configuration properties.
        """

        # Hide the 'CONFIG' command from end users by mangling its name.
        self.admin.set_config_command_name(self._mangle_config_command_name())

        self.configuration_manager.apply_system_override(
            {'daemonize': 'yes',
             'protected-mode': 'no',
             'supervised': 'systemd',
             'pidfile': system.RABBITMQ_PID_FILE,
             'logfile': system.RABBITMQ_LOG_FILE,
             'dir': system.RABBITMQ_DATA_DIR})

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        self.status.stop_db_service(
            system.SERVICE_CANDIDATES, self.state_change_wait_time,
            disable_on_boot=do_not_start_on_reboot, update_db=update_db
        )

    def restart(self):
        self.status.restart_db_service(
            system.SERVICE_CANDIDATES, self.state_change_wait_time)

    def get_config_command_name(self):
        """Get current name of the 'CONFIG' command.
        """
        renamed_cmds = self.configuration_manager.get_value('rename-command')
        for name_pair in renamed_cmds:
            if name_pair[0] == 'CONFIG':
                return name_pair[1]

        return None

    def _mangle_config_command_name(self):
        """Hide the 'CONFIG' command from the clients by renaming it to a
        random string known only to the guestagent.
        Return the mangled name.
        """
        mangled = utils.generate_random_password()
        self._rename_command('CONFIG', mangled)
        return mangled

    def _rename_command(self, old_name, new_name):
        """It is possible to completely disable a command by renaming it
        to an empty string.
        """
        self.configuration_manager.apply_system_override(
            {'rename-command': [old_name, new_name]})

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def get_configuration_property(self, name, default=None):
        """Return the value of a Rabbitmq configuration property.
        Returns a single value for single-argument properties or
        a list otherwise.
        """
        return utils.unpack_singleton(
            self.configuration_manager.get_value(name, default))

    def is_cluster_enabled(self):
        pass

    def enable_cluster(self):
        pass

    def get_cluster_config_filename(self):
        pass

    def cluster_addslots(self):
        pass

    def get_node_ip(self):
        pass

    def get_node_id_for_removal(self):
        pass

    def remove_node(self, node_ids):
        pass


class RabbitmqAdmin(object):
    """
    Handles administrative tasks in the rabbitmq database.
    """
    
    DEFAULT_CONFIG_CMD = 'CONFIG'

    def __init__(self, username=None, password=None, api_url=None):
        self.__client = api.ManagementApi(
            api_url=api_url,
            username=username,
            password=password
        )
        self.__config_cmd_name = self.DEFAULT_CONFIG_CMD

    def set_config_command_name(self, name):
        """
        Set name of the 'CONFIG' command or None for default.
        """
        self.__config_cmd_name = name or self.DEFAULT_CONFIG_CMD

    def ping(self):
        """Ping the Rabbit server and return True if a response is received.
        """
        return self.__client.aliveness_test()

    def get_info(self):
        return self.__client.overview()

    def get_nodes(self):
        return self.__client.nodes()

    def get_top(self):
        return self.__client.top()

    def get_whoami(self):
        return self.__client.whoami()

    def list_connections(self):
        return self.__client.connection.list()

    def list_exchanges(self, virtual_host='/', show_all=False):
        return self.__client.exchange.list(
            virtual_host=virtual_host, show_all=show_all)

    def list_queues(self, virtual_host='/', show_all=False):
        return self.__client.queue.list(
            virtual_host=virtual_host, show_all=show_all)

    def list_users(self):
        return self.__client.user.list()

    def list_virtual_hosts(self):
        return self.__client.virtual_host.list()
