import os
import socket
import xml.etree.ElementTree as ET

def indent(elem, level=0):
    i = "\n" + level*"  "
    j = "\n" + (level-1)*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for subelem in elem:
            indent(subelem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = j
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = j
    return elem        


def build_logger(logger_root, args):
    tags = ['level', 'path', 'err_log_path', 'size', 
              'count', 'compress', 'log_to_console']
    for tag in tags:
        if tag in args:
            elem = ET.SubElement(logger_root, tag)
            elem.text = args[tag]

def build_core_dump(core_dump_root, args):
    tags = ['size_limit']
    for tag in tags:
        if tag in args:
            elem = ET.SubElement(core_dump_root, tag)
            elem.text = args[tag]

def build_keeper(keeper_root, args):
    tags = ['my_id', 'host', 'port', 'forwarding_port', 'internal_port', 
            'log_dir', 'snapshot_dir', 'snapshot_create_interval', 'thread_count',
            'four_letter_word_white_list']
    for tag in tags:
        if tag in args:
            elem = ET.SubElement(keeper_root, tag)
            elem.text = args[tag]

    # second level element
    raft_settings = ET.SubElement(keeper_root, 'raft_settings')
    cluster = ET.SubElement(keeper_root, 'cluster')


    def build_raft_settings(raft_setting_root, args):
        tags = ['session_timeout_ms', 'operation_timeout_ms', 'dead_session_check_period_ms',
                'heart_beat_interval_ms', 'election_timeout_lower_bound_ms', 'election_timeout_upper_bound_ms',
                'reserved_log_items', 'snapshot_distance', 'max_stored_snapshots',
                'startup_timeout', 'startup_timeout', 'raft_logs_level', 'nuraft_thread_size',
                'fresh_log_gap', 'configuration_change_tries_count', 'max_batch_size', 'log_fsync_mode']

        for tag in tags:
            if tag in args:
                elem = ET.SubElement(raft_setting_root, tag)
                elem.text = args[tag]

    def build_cluster(cluster_root, args):
        fqdn = socket.getfqdn();
        for i in range(args['server']):
            elem = ET.SubElement(cluster_root, 'server')
            id = ET.SubElement(elem, 'id')
            host = ET.SubElement(elem, 'host')
            host.text = fqdn.replace(str(int(args['my_id']) - 1), str(i))
            id.text = str(i + 1)

    build_raft_settings(raft_settings, args)
    build_cluster(cluster, args)

def build_config(args):
    root = ET.Element('raftkeeper')
    # first level subelement
    logger = ET.SubElement(root, 'logger')

    build_logger(logger, args)
    core_dump = ET.SubElement(root, 'core_dump')

    keeper = ET.SubElement(root, 'keeper')
    build_keeper(keeper, args)
    
    indent(root)
    tree = ET.ElementTree(root)
    tree.write(os.getenv('RAFTKEEPER_DIR') + '/conf/config.xml', encoding='utf-8')
    # tree.write('config.xml', encoding='utf-8')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Process config parameters.')
    parser.add_argument('--server', required=True, type=int)

    # logger
    parser.add_argument('--logger.level', default='information')
    parser.add_argument('--logger.path', default='./log/raftkeeper.log')
    parser.add_argument('--logger.err_log_path', default='./log/raftkeeper.err.log')
    parser.add_argument('--logger.size', default='100M')
    parser.add_argument('--logger.count', default='10')
    parser.add_argument('--logger.compress', default='true')
    parser.add_argument('--logger.log_to_console', default='false')

    # core_dump
    parser.add_argument('--core_dump.size_limit', default='1073741824')
    
    # keeper
    # my_id, host, port, forwarding_port, internal_port are fixed
    parser.add_argument('--keeper.log_dir', default='./data/log')
    parser.add_argument('--keeper.snapshort_dir', default='./data/snapshot')
    parser.add_argument('--keeper.snapshot_create_interval', default='3600')
    parser.add_argument('--keeper.thread_count', default='16')
    parser.add_argument('--keeper.four_letter_word_white_list', default='conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld')
    parser.add_argument('--keeper.super_digest', default='')

    parser.add_argument('--keeper.raft_settings.session_timeout_ms', default='30000')
    parser.add_argument('--keeper.raft_settings.operation_timeout_ms', default='20000')
    parser.add_argument('--keeper.raft_settings.dead_session_check_period_ms', default='100')
    parser.add_argument('--keeper.raft_settings.heart_beat_interval_ms', default='500')
    parser.add_argument('--keeper.raft_settings.election_timeout_lower_bound_ms', default='10000')
    parser.add_argument('--keeper.raft_settings.election_timeout_upper_bound_ms', default='20000')
    parser.add_argument('--keeper.raft_settings.reserved_log_items', default='1000000')
    parser.add_argument('--keeper.raft_settings.snapshot_distance', default='3000000')
    parser.add_argument('--keeper.raft_settings.max_stored_snapshots', default='5')
    parser.add_argument('--keeper.raft_settings.startup_timeout', default='6000000')
    parser.add_argument('--keeper.raft_settings.shutdown_timeout', default='5000')
    parser.add_argument('--keeper.raft_settings.raft_logs_level', default='information')
    parser.add_argument('--keeper.raft_settings.nuraft_thread_size', default='32')
    parser.add_argument('--keeper.raft_settings.fresh_log_gap', default='200')
    parser.add_argument('--keeper.raft_settings.configuration_change_tries_count', default='30')
    parser.add_argument('--keeper.raft_settings.max_batch_size', default='1000')
    parser.add_argument('--keeper.raft_settings.log_fsync_mode', default='fsync_parallel')

    args = {k.split('.')[-1] : v for k, v in vars(parser.parse_args()).items()}
    print(args)

    hostname = socket.gethostname()
    server_id = str(int(hostname.split('-')[-1]) + 1)

    # values below are fixed
    args['my_id'] = server_id
    args['port'] = '8101'
    args['forwarding_port'] = '8102'
    args['internal_port'] = '8103'
    args['host'] = socket.getfqdn()
    
    print(args)
    build_config(args)
