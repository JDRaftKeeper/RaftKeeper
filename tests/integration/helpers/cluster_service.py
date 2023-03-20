# Copyright 2016-2021 ClickHouse, Inc.
# Copyright 2021-2023 JD.com, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import errno
import logging
import os
import os.path as p
import pprint
import pwd
import re
import shutil
import socket
import subprocess
import time
import traceback

import docker
import xml.dom.minidom
from dicttoxml import dicttoxml
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException

# from .client import Client

HELPERS_DIR = p.dirname(__file__)
RAFTKEEPER_ROOT_DIR = p.join(p.dirname(__file__), "../../..")
LOCAL_DOCKER_COMPOSE_DIR = p.join(RAFTKEEPER_ROOT_DIR, "docker/test/integration/runner/compose/")
DEFAULT_ENV_NAME = 'env_file'

SANITIZER_SIGN = "=================="


def _create_env_file(path, variables, fname=DEFAULT_ENV_NAME):
    full_path = os.path.join(path, fname)
    with open(full_path, 'w') as f:
        for var, value in list(variables.items()):
            f.write("=".join([var, value]) + "\n")
    return full_path

def subprocess_check_call(args):
    # Uncomment for debugging
    # print('run:', ' ' . join(args))
    subprocess.check_call(args)


def subprocess_call(args):
    # Uncomment for debugging..;
    # print('run:', ' ' . join(args))
    subprocess.call(args)

def run_and_check(args, env=None, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, nothrow=False, detach=False):
    if detach:
        subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env, shell=shell)
        return

    logging.debug(f"Command:{args}")
    res = subprocess.run(args, stdout=stdout, stderr=stderr, env=env, shell=shell, timeout=timeout)
    out = res.stdout.decode('utf-8')
    err = res.stderr.decode('utf-8')
    # check_call(...) from subprocess does not print stderr, so we do it manually
    for outline in out.splitlines():
        logging.debug(f"Stdout:{outline}")
    for errline in err.splitlines():
        logging.debug(f"Stderr:{errline}")
    if res.returncode != 0:
        logging.debug(f"Exitcode:{res.returncode}")
        if env:
            logging.debug(f"Env:{env}")
        if not nothrow:
            raise Exception(f"Command {args} return non-zero code {res.returncode}: {res.stderr.decode('utf-8')}")
    return out


def get_docker_compose_path():
    compose_path = os.environ.get('DOCKER_COMPOSE_DIR')
    if compose_path is not None:
        return os.path.dirname(compose_path)
    else:
        if os.path.exists(os.path.dirname('/compose/')):
            return os.path.dirname('/compose/')  # default in docker runner container
        else:
            print(("Fallback docker_compose_path to LOCAL_DOCKER_COMPOSE_DIR: {}".format(LOCAL_DOCKER_COMPOSE_DIR)))
            return LOCAL_DOCKER_COMPOSE_DIR


class RaftKeeperCluster:
    """RaftKeeper cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, RaftKeeper configs etc.
    """

    def __init__(self, base_path, name=None, base_config_dir=None, server_bin_path=None, zookeeper_config_path=None, custom_dockerd_host=None):
        for param in list(os.environ.keys()):
            print("ENV %40s %s" % (param, os.environ[param]))
        self.base_dir = p.dirname(base_path)
        self.name = name if name is not None else ''

        self.base_config_dir = base_config_dir or os.environ.get('RAFTKEEPER_TESTS_BASE_CONFIG_DIR',
                                                                 '/etc/raftkeeper-server/')

        self.server_bin_path = p.realpath(
            server_bin_path or os.environ.get('RAFTKEEPER_TESTS_SERVER_BIN_PATH', '/usr/bin/raftkeeper'))

        self.zookeeper_config_path = p.join(self.base_dir, zookeeper_config_path) if zookeeper_config_path else p.join(
            HELPERS_DIR, 'zookeeper_config.xml')

        self.project_name = pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir) + self.name
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())
        self.instances_dir = p.join(self.base_dir, '_instances' + ('' if not self.name else '_' + self.name))
        self.docker_logs_path = p.join(self.instances_dir, 'docker.log')

        custom_dockerd_host = custom_dockerd_host or os.environ.get('RAFTKEEPER_TESTS_DOCKERD_HOST')
        self.docker_api_version = os.environ.get("DOCKER_API_VERSION")
        self.docker_base_tag = os.environ.get("DOCKER_BASE_TAG", "latest")

        self.base_cmd = ['docker-compose']
        if custom_dockerd_host:
            self.base_cmd += ['--host', custom_dockerd_host]

        self.base_cmd += ['--project-directory', self.base_dir, '--project-name', self.project_name]
        self.base_zookeeper_cmd = None
        self.pre_zookeeper_commands = []
        self.instances = {}
        self.with_zookeeper = False
        self.with_net_trics = False

        self.zookeeper_use_tmpfs = True

        self.docker_client = None
        self.is_up = False
        print("CLUSTER INIT base_config_dir:{}".format(self.base_config_dir))

    def add_instance(self, name, base_config_dir=None, main_configs=None,
                     with_zookeeper=False,
                     raftkeeper_path_dir=None,
                     hostname=None, env_variables=None, image="raftkeeper/raftkeeper-integration-tests", tag=None,
                     stay_alive=False, ipv4_address=None, ipv6_address=None, with_installed_binary=False, tmpfs=None,
                     zookeeper_docker_compose_path=None, zookeeper_use_tmpfs=True, use_old_bin=False):
        """Add an instance to the cluster.

        name - the name of the instance directory and the value of the 'instance' macro in RaftKeeper.
        base_config_dir - a directory with config.xml and users.xml files which will be copied to /etc/raftkeeper-server/ directory
        main_configs - a list of config files that will be added to config.d/ directory
        user_configs - a list of config files that will be added to users.d/ directory
        with_zookeeper - if True, add ZooKeeper configuration to configs and ZooKeeper instances to the cluster.
        """

        if self.is_up:
            raise Exception("Can\'t add instance %s: cluster is already up!" % name)

        if name in self.instances:
            raise Exception("Can\'t add instance `%s': there is already an instance with the same name!" % name)

        if tag is None:
            tag = self.docker_base_tag
        if not env_variables:
            env_variables = {}

        # Code coverage files will be placed in database directory
        # (affect only WITH_COVERAGE=1 build)
        env_variables['LLVM_PROFILE_FILE'] = '/var/lib/raftkeeper/server_%h_%p_%m.profraw'

        instance = RaftKeeperInstance(
            cluster=self,
            base_path=self.base_dir,
            name=name,
            base_config_dir=base_config_dir if base_config_dir else self.base_config_dir,
            custom_main_configs=main_configs or [],
            with_zookeeper=with_zookeeper,
            zookeeper_config_path=self.zookeeper_config_path,
            server_bin_path=self.server_bin_path,
            raftkeeper_path_dir=raftkeeper_path_dir,
            hostname=hostname,
            env_variables=env_variables,
            image=image,
            tag=tag,
            stay_alive=stay_alive,
            ipv4_address=ipv4_address,
            ipv6_address=ipv6_address,
            with_installed_binary=with_installed_binary,
            tmpfs=tmpfs or [],
            use_old_bin=use_old_bin)

        docker_compose_yml_dir = get_docker_compose_path()

        self.instances[name] = instance
        if ipv4_address is not None or ipv6_address is not None:
            self.with_net_trics = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_net.yml')])

        self.base_cmd.extend(['--file', instance.docker_compose_path])

        cmds = []
        if with_zookeeper and not self.with_zookeeper:
            if not zookeeper_docker_compose_path:
                zookeeper_docker_compose_path = p.join(docker_compose_yml_dir, 'docker_compose_zookeeper.yml')

            self.with_zookeeper = True
            self.zookeeper_use_tmpfs = zookeeper_use_tmpfs
            self.base_cmd.extend(['--file', zookeeper_docker_compose_path])
            self.base_zookeeper_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', zookeeper_docker_compose_path]
            cmds.append(self.base_zookeeper_cmd)

        if self.with_net_trics:
            for cmd in cmds:
                cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_net.yml')])

        print("Cluster name:{} project_name:{}. Added instance name:{} tag:{} base_cmd:{} docker_compose_yml_dir:{}".format(
            self.name, self.project_name, name, tag, self.base_cmd, docker_compose_yml_dir))
        return instance

    def get_instance_docker_id(self, instance_name):
        # According to how docker-compose names containers.
        return self.project_name + '_' + instance_name + '_1'

    def _replace(self, path, what, to):
        with open(path, 'r') as p:
            data = p.read()
        data = data.replace(what, to)
        with open(path, 'w') as p:
            p.write(data)

    def copy_file_from_container_to_container(self, src_node, src_path, dst_node, dst_path):
        fname = os.path.basename(src_path)
        run_and_check([f"docker cp {src_node.docker_id}:{src_path} {self.instances_dir}"], shell=True)
        run_and_check([f"docker cp {self.instances_dir}/{fname} {dst_node.docker_id}:{dst_path}"], shell=True)

    def restart_instance_with_ip_change(self, node, new_ip):
        if '::' in new_ip:
            if node.ipv6_address is None:
                raise Exception("You should specity ipv6_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv6_address, new_ip)
            node.ipv6_address = new_ip
        else:
            if node.ipv4_address is None:
                raise Exception("You should specity ipv4_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv4_address, new_ip)
            node.ipv4_address = new_ip
        subprocess.check_call(self.base_cmd + ["stop", node.name])
        subprocess.check_call(self.base_cmd + ["rm", "--force", "--stop", node.name])
        subprocess.check_call(self.base_cmd + ["up", "--force-recreate", "--no-deps", "-d", node.name])
        node.ip_address = self.get_instance_ip(node.name)
        # node.client = Client(node.ip_address, command=self.client_bin_path)
        start_deadline = time.time() + 180.0  # seconds
        node.wait_for_start(start_deadline)
        return node

    def get_instance_ip(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return list(handle.attrs['NetworkSettings']['Networks'].values())[0]['IPAddress']

    def get_container_id(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return handle.attrs['Id']

    def get_container_logs(self, instance_name):
        container_id = self.get_container_id(instance_name)
        return self.docker_client.api.logs(container_id).decode()

    def exec_in_container(self, container_id, cmd, detach=False, nothrow=False, **kwargs):
        exec_id = self.docker_client.api.exec_create(container_id, cmd, **kwargs)
        output = self.docker_client.api.exec_start(exec_id, detach=detach)

        exit_code = self.docker_client.api.exec_inspect(exec_id)['ExitCode']
        if exit_code:
            container_info = self.docker_client.api.inspect_container(container_id)
            image_id = container_info.get('Image')
            image_info = self.docker_client.api.inspect_image(image_id)
            print(("Command failed in container {}: ".format(container_id)))
            pprint.pprint(container_info)
            print("")
            print(("Container {} uses image {}: ".format(container_id, image_id)))
            pprint.pprint(image_info)
            print("")
            message = 'Cmd "{}" failed in container {}. Return code {}. Output: {}'.format(' '.join(cmd), container_id,
                                                                                           exit_code, output)
            if nothrow:
                print(message)
            else:
                raise Exception(message)
        if not detach:
            return output.decode()
        return output

    def copy_file_to_container(self, container_id, local_path, dest_path):
        with open(local_path, "r") as fdata:
            data = fdata.read()
            encodedBytes = base64.b64encode(data.encode("utf-8"))
            encodedStr = str(encodedBytes, "utf-8")
            self.exec_in_container(container_id,
                                   ["bash", "-c", "echo {} | base64 --decode > {}".format(encodedStr, dest_path)],
                                   user='root')

    def wait_zookeeper_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in ['zoo1', 'zoo2', 'zoo3']:
                    conn = self.get_kazoo_client(instance)
                    conn.get_children('/')
                print("All instances of ZooKeeper started")
                return
            except Exception as ex:
                print("Can't connect to ZooKeeper " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait ZooKeeper container")

    def start(self, destroy_dirs=True):
        print("Cluster start called. is_up={}, destroy_dirs={}".format(self.is_up, destroy_dirs))
        if self.is_up:
            return

        # Just in case kill unstopped containers from previous launch
        try:
            print("Trying to kill unstopped containers...")

            if not subprocess_call(['docker-compose', 'kill']):
                subprocess_call(['docker-compose', 'down', '--volumes'])
            print("Unstopped containers killed")
        except:
            pass

        try:
            if destroy_dirs and p.exists(self.instances_dir):
                print(("Removing instances dir %s", self.instances_dir))
                shutil.rmtree(self.instances_dir)

            for instance in list(self.instances.values()):
                print(('Setup directory for instance: {} destroy_dirs: {}'.format(instance.name, destroy_dirs)))
                instance.create_dir(destroy_dir=destroy_dirs)

            self.docker_client = docker.from_env(version=self.docker_api_version)

            common_opts = ['up', '-d', '--force-recreate']

            if self.with_zookeeper and self.base_zookeeper_cmd:
                print('Setup ZooKeeper')
                env = os.environ.copy()
                if not self.zookeeper_use_tmpfs:
                    env['ZK_FS'] = 'bind'
                    for i in range(1, 4):
                        zk_data_path = self.instances_dir + '/zkdata' + str(i)
                        zk_log_data_path = self.instances_dir + '/zklog' + str(i)
                        if not os.path.exists(zk_data_path):
                            os.mkdir(zk_data_path)
                        if not os.path.exists(zk_log_data_path):
                            os.mkdir(zk_log_data_path)
                        env['ZK_DATA' + str(i)] = zk_data_path
                        env['ZK_DATA_LOG' + str(i)] = zk_log_data_path
                subprocess.check_call(self.base_zookeeper_cmd + common_opts, env=env)
                for command in self.pre_zookeeper_commands:
                    self.run_kazoo_commands_with_retries(command, repeats=5)
                self.wait_zookeeper_to_start(120)

            raftkeeper_start_cmd = self.base_cmd + ['up', '-d', '--no-recreate']
            print(("Trying to create RaftKeeper instance by command %s", ' '.join(map(str, raftkeeper_start_cmd))))
            subprocess.check_output(raftkeeper_start_cmd)
            print("RaftKeeper instance created")

            start_deadline = time.time() + 180.0  # seconds
            for instance in self.instances.values():
                instance.docker_client = self.docker_client
                instance.ip_address = self.get_instance_ip(instance.name)

                print("Waiting for RaftKeeper-server start...")
                instance.wait_for_start(start_deadline)
                print("RaftKeeper-server started")

            self.is_up = True
            # wait cluster init TODO use a deterministic way
            time.sleep(5)
            print("RaftKeeper Cluster started!")

        except BaseException as e:
            print("Failed to start cluster: ")
            print(str(e))
            print(traceback.print_exc())
            raise

    def shutdown(self, kill=True):
        sanitizer_assert_instance = None
        with open(self.docker_logs_path, "w+") as f:
            try:
                subprocess.check_call(self.base_cmd + ['logs'], stdout=f)
            except Exception as e:
                print("Unable to get logs from docker.")
            f.seek(0)
            for line in f:
                if SANITIZER_SIGN in line:
                    sanitizer_assert_instance = line.split('|')[0].strip()
                    break

        if kill:
            try:
                subprocess_check_call(self.base_cmd + ['stop', '--timeout', '20'])
            except Exception as e:
                print("Kill command failed during shutdown. {}".format(repr(e)))
                print("Trying to kill forcefully")
                subprocess_check_call(self.base_cmd + ['kill'])

        try:
            subprocess_check_call(self.base_cmd + ['down', '--volumes', '--remove-orphans'])
        except Exception as e:
            print("Down + remove orphans failed durung shutdown. {}".format(repr(e)))

        self.is_up = False

        self.docker_client = None

        for instance in list(self.instances.values()):
            instance.docker_client = None
            instance.ip_address = None
            # instance.client = None

        if not self.zookeeper_use_tmpfs:
            for i in range(1, 4):
                zk_data_path = self.instances_dir + '/zkdata' + str(i)
                zk_log_data_path = self.instances_dir + '/zklog' + str(i)
                if os.path.exists(zk_data_path):
                    shutil.rmtree(zk_data_path)
                if os.path.exists(zk_log_data_path):
                    shutil.rmtree(zk_log_data_path)

        if sanitizer_assert_instance is not None:
            raise Exception(
                "Sanitizer assert found in {} for instance {}".format(self.docker_logs_path, sanitizer_assert_instance))

    def pause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ['pause', instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGSTOP', instance_name])

    def unpause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ['unpause', instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGCONT', instance_name])

    def open_bash_shell(self, instance_name):
        os.system(' '.join(self.base_cmd + ['exec', instance_name, '/bin/bash']))

    def get_kazoo_client(self, zoo_instance_name):
        zk = KazooClient(hosts=self.get_instance_ip(zoo_instance_name))
        zk.start()
        return zk

    def run_kazoo_commands_with_retries(self, kazoo_callback, zoo_instance_name='zoo1', repeats=1, sleep_for=1):
        for i in range(repeats - 1):
            try:
                kazoo_callback(self.get_kazoo_client(zoo_instance_name))
                return
            except KazooException as e:
                print(repr(e))
                time.sleep(sleep_for)

        kazoo_callback(self.get_kazoo_client(zoo_instance_name))

    def add_zookeeper_startup_command(self, command):
        self.pre_zookeeper_commands.append(command)

    def stop_zookeeper_nodes(self, zk_nodes):
        for n in zk_nodes:
            logging.info("Stopping zookeeper node: %s", n)
            subprocess_check_call(self.base_zookeeper_cmd + ["stop", n])

    def start_zookeeper_nodes(self, zk_nodes):
        for n in zk_nodes:
            logging.info("Starting zookeeper node: %s", n)
            subprocess_check_call(self.base_zookeeper_cmd + ["start", n])


RAFTKEEPER_START_COMMAND = "raftkeeper server --config-file=/etc/raftkeeper-server/config.xml --log-file=/var/log/raftkeeper-server/raftkeeper-server.log --errorlog-file=/var/log/raftkeeper-server/raftkeeper-server.err.log"
OLD_RAFTKEEPER_START_COMMAND = "raftkeeper_old server --config-file=/etc/raftkeeper-server/config.xml --log-file=/var/log/raftkeeper-server/raftkeeper-server.log --errorlog-file=/var/log/raftkeeper-server/raftkeeper-server.err.log"

RAFTKEEPER_STAY_ALIVE_COMMAND = 'bash -c "{} --daemon; tail -f /dev/null"'.format(RAFTKEEPER_START_COMMAND)
OLD_RAFTKEEPER_STAY_ALIVE_COMMAND = 'bash -c "{} --daemon; tail -f /dev/null"'.format(OLD_RAFTKEEPER_START_COMMAND)

DOCKER_COMPOSE_TEMPLATE = '''
version: '2.3'
services:
    {name}:
        image: {image}:{tag}
        hostname: {hostname}
        volumes:
            - {instance_config_dir}:/etc/raftkeeper-server/
            - {logs_dir}:/var/log/raftkeeper-server/
            - /etc/passwd:/etc/passwd:ro
            {binary_volume}
            {old_binary_volume}
        entrypoint: {entrypoint_cmd}
        tmpfs: {tmpfs}
        cap_add:
            - SYS_PTRACE
            - NET_ADMIN
        depends_on: {depends_on}
        user: '{user}'
        env_file:
            - {env_file}
        security_opt:
            - label:disable
        dns_opt:
            - attempts:2
            - timeout:1
            - inet6
            - rotate
        {networks}
            {app_net}
                {ipv4_address}
                {ipv6_address}
                {net_aliases}
                    {net_alias1}
'''


class RaftKeeperInstance:

    def __init__(
            self, cluster, base_path, name, base_config_dir, custom_main_configs, with_zookeeper, zookeeper_config_path, server_bin_path, raftkeeper_path_dir,
            hostname=None, env_variables=None,
            image="raftkeeper/raftkeeper-integration-tests", tag="latest",
            stay_alive=False, ipv4_address=None, ipv6_address=None, with_installed_binary=False, tmpfs=None, use_old_bin=False):

        self.name = name
        self.base_cmd = cluster.base_cmd
        self.docker_id = cluster.get_instance_docker_id(self.name)
        self.cluster = cluster
        self.hostname = hostname if hostname is not None else self.name

        self.tmpfs = tmpfs or []
        self.use_old_bin = use_old_bin
        self.base_config_dir = p.abspath(p.join(base_path, base_config_dir)) if base_config_dir else None
        self.custom_main_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_main_configs]
        self.raftkeeper_path_dir = p.abspath(p.join(base_path, raftkeeper_path_dir)) if raftkeeper_path_dir else None

        self.with_zookeeper = with_zookeeper
        self.zookeeper_config_path = zookeeper_config_path

        self.server_bin_path = server_bin_path

        self.path = p.join(self.cluster.instances_dir, name)
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')
        self.env_variables = env_variables or {}

        self.docker_client = None
        self.ip_address = None
        self.client = None
        self.default_timeout = 20.0  # 20 sec
        self.image = image
        self.tag = tag
        self.stay_alive = stay_alive
        self.ipv4_address = ipv4_address
        self.ipv6_address = ipv6_address
        self.with_installed_binary = with_installed_binary

    def kill_raftkeeper(self, stop_start_wait_sec=3):
        if self.use_old_bin:
            pid = self.get_process_pid("raftkeeper_old")
        else:
            pid = self.get_process_pid("raftkeeper")

        if not pid:
            raise Exception("No raftkeeper found")
        self.exec_in_container(["bash", "-c", "kill -9 {}".format(pid)], user='root')
        time.sleep(stop_start_wait_sec)

    def restore_raftkeeper(self, retries=100):
        if self.use_old_bin:
            pid = self.get_process_pid("raftkeeper_old")
        else:
            pid = self.get_process_pid("raftkeeper")

        if pid:
            raise Exception("RaftKeeper has already started")
        if self.use_old_bin:
            self.exec_in_container(["bash", "-c", "{} --daemon".format(OLD_RAFTKEEPER_START_COMMAND)], user=str(os.getuid()))
        else:
            self.exec_in_container(["bash", "-c", "{} --daemon".format(RAFTKEEPER_START_COMMAND)], user=str(os.getuid()))
        # from helpers.test_tools import assert_eq_with_retry
        # wait start
        # assert_eq_with_retry(self, "select 1", "1", retry_count=retries)

    def stop_raftkeeper(self, stop_wait_sec=30, kill=False):
        if not self.stay_alive:
            raise Exception("raftkeeper can be stopped only with stay_alive=True instance")
        try:
            if self.use_old_bin:
                pid = self.get_process_pid("raftkeeper_old")
                print("use_old_bin")
            else:
                pid = self.get_process_pid("raftkeeper")

            if pid is None:
                logging.warning("RaftKeeper process already stopped")
                return

            # self.kill_raftkeeper()
            output = self.exec_in_container(["bash", "-c", "kill {} {}".format("-9" if kill else "", pid)], user='root')
            print(f"kill raftkeeper pid:{pid}, force:{kill}, command output:{output}")

            start_time = time.time()
            stopped = False
            while time.time() <= start_time + stop_wait_sec:
                if self.use_old_bin:
                    pid = self.get_process_pid("raftkeeper_old")
                else:
                    pid = self.get_process_pid("raftkeeper")

                print("stop raftkeeper found pid ", pid)
                if pid is None:
                    stopped = True
                    break
                else:
                    time.sleep(1)

            if not stopped:
                print(self.name, "stop failed")
                if self.use_old_bin:
                    pid = self.get_process_pid("raftkeeper_old")
                else:
                    pid = self.get_process_pid("raftkeeper")

                if pid is not None:
                    logging.warning(f"Force kill raftkeeper in stop_raftkeeper. pid:{pid}")
                    self.kill_raftkeeper()
                else:
                    ps_all = self.exec_in_container(["bash", "-c", "ps aux"], nothrow=True, user='root')
                    logging.warning(f"We want force stop raftkeeper, but no raftkeeper-server is running\n{ps_all}")
                    return
            else:
                print(self.name, "stopped")
        except Exception as e:
            logging.warning(f"Stop RaftKeeper raised an error {e}")

    def start_raftkeeper(self, start_wait_sec=60, start_wait=True):
        if not self.stay_alive:
            raise Exception("RaftKeeper can be started again only with stay_alive=True instance")
        start_time = time.time()
        time_to_sleep = 0.5

        while start_time + start_wait_sec >= time.time():
            # sometimes after SIGKILL (hard reset) server may refuse to start for some time
            # for different reasons.
            if self.use_old_bin:
                pid = self.get_process_pid("raftkeeper_old")
            else:
                pid = self.get_process_pid("raftkeeper")

            if pid is None:
                logging.debug("No raftkeeper process running. Start new one.")
                print("No raftkeeper process running. Start new one.")
                if self.use_old_bin:
                    self.exec_in_container(["bash", "-c", "{} --daemon".format(OLD_RAFTKEEPER_START_COMMAND)], user=str(os.getuid()))
                else:
                    self.exec_in_container(["bash", "-c", "{} --daemon".format(RAFTKEEPER_START_COMMAND)], user=str(os.getuid()))
                time.sleep(1)
                continue
            elif start_wait is True:
                logging.debug("RaftKeeper process running.")
                print("RaftKeeper process running.")
                try:
                    self.wait_start(start_wait_sec)
                    return
                except Exception as e:
                    logging.warning(f"Current start attempt failed. Will kill {pid} just in case.")
                    self.exec_in_container(["bash", "-c", f"kill -9 {pid}"], user='root', nothrow=True)
                    time.sleep(time_to_sleep)
            elif start_wait is False:
                return

        raise Exception("Cannot start RaftKeeper, see additional info in logs")

    def wait_start(self, start_wait_sec=30):
        start_time = time.time()
        while start_time + start_wait_sec >= time.time():
            zk = None
            try:
                zk = self.get_fake_zk(start_wait_sec)
                zk.get("/")
                print("node", self.name, "ready")
                break
            except Exception as ex:
                time.sleep(0.5)
                print("Waiting until", self.name, "will be ready, exception", ex)
            finally:
                if zk:
                    zk.stop()
                    zk.close()
        raise Exception("Can't wait node", self.name, "to become ready")

    def get_fake_zk(self, session_timeout=10):
        _fake_zk_instance = KazooClient(hosts=self.ip_address + ":8101", timeout=session_timeout)
        def reset_listener(state):
            nonlocal _fake_zk_instance
            # print("Fake zk callback called for state", state)
            if state != KazooState.CONNECTED:
                _fake_zk_instance._reset()

        _fake_zk_instance.add_listener(reset_listener)
        _fake_zk_instance.start()
        return _fake_zk_instance

    def restart_raftkeeper(self, stop_start_wait_sec=60, kill=False):
        self.stop_raftkeeper(stop_start_wait_sec, kill)
        self.start_raftkeeper(stop_start_wait_sec)

    def replace_in_config(self, path_to_config, replace, replacement):
        self.exec_in_container(["bash", "-c", f"sed -i 's/{replace}/{replacement}/g' {path_to_config}"])

    def exec_in_container(self, cmd, detach=False, nothrow=False, **kwargs):
        container_id = self.get_docker_handle().id
        return self.cluster.exec_in_container(container_id, cmd, detach, nothrow, **kwargs)

    def contains_in_log(self, substring):
        result = self.exec_in_container(
            ["bash", "-c", 'grep "{}" /var/log/raftkeeper-server/raftkeeper-server.log || true'.format(substring)])
        return len(result) > 0

    def file_exists(self, path):
        return self.exec_in_container(
            ["bash", "-c", "echo $(if [ -e '{}' ]; then echo 'yes'; else echo 'no'; fi)".format(path)]) == 'yes\n'

    def copy_file_to_container(self, local_path, dest_path):
        container_id = self.get_docker_handle().id
        return self.cluster.copy_file_to_container(container_id, local_path, dest_path)

    def get_process_pid(self, process_name):
        output1 = self.exec_in_container(["bash", "-c", "ps ax | grep '{}'".format(process_name)])
        print(f"get_process_pid output {output1}")
        output = self.exec_in_container(["bash", "-c",
                                         "ps ax | grep '{}' | grep -v 'grep' | grep -v 'bash -c' | awk '{{print $1}}'".format(
                                             process_name)])

        if output:
            try:
                pid = int(output.split('\n')[0].strip())
                return pid
            except:
                return None
        return None

    def get_docker_handle(self):
        return self.docker_client.containers.get(self.docker_id)

    def stop(self):
        self.get_docker_handle().stop()

    def start(self):
        self.get_docker_handle().start()

    def wait_for_start(self, deadline=None, timeout=None):
        start_time = time.time()

        if timeout is not None:
            deadline = start_time + timeout

        while True:
            handle = self.get_docker_handle()
            status = handle.status
            if status == 'exited':
                raise Exception(
                    "Instance `{}' failed to start. Container status: {}, logs: {}".format(self.name, status,
                                                                                           handle.logs()))

            current_time = time.time()
            time_left = deadline - current_time
            if deadline is not None and current_time >= deadline:
                raise Exception("Timed out while waiting for instance `{}' with ip address {} to start. "
                                "Container status: {}, logs: {}".format(self.name, self.ip_address, status,
                                                                        handle.logs()))

            # Repeatedly poll the instance address until there is something that listens there.
            # Usually it means that RaftKeeper is ready to accept queries.
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(time_left)
                sock.connect((self.ip_address, 8101))
                return
            except socket.timeout:
                continue
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED or e.errno == errno.EHOSTUNREACH or e.errno == errno.ENETUNREACH:
                    time.sleep(0.1)
                else:
                    raise
            finally:
                sock.close()

    @staticmethod
    def dict_to_xml(dictionary):
        xml_str = dicttoxml(dictionary, custom_root="yandex", attr_type=False)
        return xml.dom.minidom.parseString(xml_str).toprettyxml()

    def replace_config(self, path_to_config, replacement):
        self.exec_in_container(["bash", "-c", "echo '{}' > {}".format(replacement, path_to_config)])

    def create_dir(self, destroy_dir=True):
        """Create the instance directory and all the needed files there."""

        if destroy_dir:
            self.destroy_dir()
        elif p.exists(self.path):
            return

        os.makedirs(self.path)

        instance_config_dir = p.abspath(p.join(self.path, 'configs'))
        os.makedirs(instance_config_dir)

        print("Copy common default production configuration from {}".format(self.base_config_dir))
        shutil.copyfile(p.join(self.base_config_dir, 'config.xml'), p.join(instance_config_dir, 'config.xml'))

        print("Create directory for configuration generated in this helper")
        # used by all utils with any config
        conf_d_dir = p.abspath(p.join(instance_config_dir, 'conf.d'))
        os.mkdir(conf_d_dir)

        print("Create directory for common tests configuration")
        # used by server with main config.xml
        self.config_d_dir = p.abspath(p.join(instance_config_dir, 'config.d'))
        os.mkdir(self.config_d_dir)

        print("Copy common configuration from helpers")
        # The file is named with 0_ prefix to be processed before other configuration overloads.
        shutil.copy(p.join(HELPERS_DIR, '0_common_instance_config.xml'), self.config_d_dir)

        # Put ZooKeeper config
        if self.with_zookeeper:
            shutil.copy(self.zookeeper_config_path, conf_d_dir)

        # Copy config.d configs
        print("Copy custom test config files {} to {}".format(self.custom_main_config_paths, self.config_d_dir))
        for path in self.custom_main_config_paths:
            shutil.copy(path, self.config_d_dir)

        logs_dir = p.abspath(p.join(self.path, 'logs'))
        print("Setup logs dir {}".format(logs_dir))
        os.mkdir(logs_dir)

        depends_on = []

        if self.with_zookeeper:
            depends_on.append("zoo1")
            depends_on.append("zoo2")
            depends_on.append("zoo3")

        env_file = _create_env_file(os.path.dirname(self.docker_compose_path), self.env_variables)

        print("Env {} stored in {}".format(self.env_variables, env_file))

        if self.use_old_bin:
            entrypoint_cmd = OLD_RAFTKEEPER_START_COMMAND
        else:
            entrypoint_cmd = RAFTKEEPER_START_COMMAND

        if self.stay_alive:
            if self.use_old_bin:
                entrypoint_cmd = OLD_RAFTKEEPER_STAY_ALIVE_COMMAND
            else:
                entrypoint_cmd = RAFTKEEPER_STAY_ALIVE_COMMAND

        print("Entrypoint cmd: {}".format(entrypoint_cmd))

        networks = app_net = ipv4_address = ipv6_address = net_aliases = net_alias1 = ""
        if self.ipv4_address is not None or self.ipv6_address is not None or self.hostname != self.name:
            networks = "networks:"
            app_net = "default:"
            if self.ipv4_address is not None:
                ipv4_address = "ipv4_address: " + self.ipv4_address
            if self.ipv6_address is not None:
                ipv6_address = "ipv6_address: " + self.ipv6_address
            if self.hostname != self.name:
                net_aliases = "aliases:"
                net_alias1 = "- " + self.hostname


        old_binary_volume = "- " + "/raftkeeper_old" + ":/usr/bin/raftkeeper_old"
        binary_volume = "- " + self.server_bin_path + ":/usr/bin/raftkeeper"


        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                image=self.image,
                tag=self.tag,
                name=self.name,
                hostname=self.hostname,
                binary_volume=binary_volume,
                old_binary_volume=old_binary_volume,
                instance_config_dir=instance_config_dir,
                config_d_dir=self.config_d_dir,
                tmpfs=str(self.tmpfs),
                logs_dir=logs_dir,
                depends_on=str(depends_on),
                user=os.getuid(),
                env_file=env_file,
                entrypoint_cmd=entrypoint_cmd,
                networks=networks,
                app_net=app_net,
                ipv4_address=ipv4_address,
                ipv6_address=ipv6_address,
                net_aliases=net_aliases,
                net_alias1=net_alias1,
            ))

    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)


class RaftKeeperKiller(object):
    def __init__(self, raftkeeper_node):
        self.raftkeeper_node = raftkeeper_node

    def __enter__(self):
        self.raftkeeper_node.kill_raftkeeper()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.raftkeeper_node.restore_raftkeeper()
