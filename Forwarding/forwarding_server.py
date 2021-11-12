import asyncio
import logging
import os
import networkx as nx
import time
from communication_standards import Correct_Answer
from communication_standards import Error_Answer

formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')
og_chunk_size = 1024
host = "0.0.0.0"
port = 1981
clients_dict = {}
clients_file_route = '../Servers'
init_route_file = '../Routing_Info'
routing_file_route = '../Routing_Updates'


def get_clients():
    temp_dict = {}
    with open(clients_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            node_info = line.split(":")
            temp_dict[node_info[0]] = node_info[1]
    return temp_dict


def get_name():
    with open(init_route_file, 'r', encoding='UTF-8') as file:
        name_line = file.readline()
    return name_line[4:].strip()


def get_first_net(logger):
    empty_graph = os.stat(routing_file_route).st_size == 0
    while empty_graph:
        time.sleep(1)
        empty_graph = os.stat(routing_file_route).st_size == 0
    logger.info('DV graph modified, update!')
    return nx.read_gpickle(routing_file_route)


def setup_logger(name, log_file, clean_file=False, level=logging.DEBUG):
    if not os.path.exists(log_file):
        os.mknod(log_file)
    if clean_file:
        open(log_file, 'w').close()

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.addHandler(console_handler)

    return logger


def serve_client_cb(client_reader, client_writer):
    client_id = client_writer.get_extra_info('peername')
    client_ip = client_id[0]
    client_name = ''

    for temp_client_name in clients_dict.keys():
        if clients_dict.get(temp_client_name) == client_ip:
            client_name = temp_client_name

    if not client_name:
        outer_logger.error('Not found client IP tried to connect, ip: {}'.format(client_ip))
        return

    client_logger = setup_logger(client_name, '../Logs/routing_server_to_{}'.format(client_name))
    client_logger.info('Client connected: {}'.format(client_id))

    asyncio.ensure_future(client_task(client_reader, client_writer, client_logger))


async def client_task(reader, writer, logger):
    client_addr = writer.get_extra_info('peername')
    logger.info('Start echoing back to {}'.format(client_addr))
    keep_waiting = True
    while keep_waiting:
        data = await reader.read(1024)
        print(data)


if __name__ == '__main__':
    outer_logger = setup_logger('outer_logger', '../Logs/forwarding_server_outer')
    my_name = get_name()
    net = get_first_net(outer_logger)
    clients_dict = get_clients()
    loop = asyncio.get_event_loop()
    server_core = asyncio.start_server(serve_client_cb,
                                       host=host,
                                       port=port,
                                       loop=loop)
    server = loop.run_until_complete(server_core)

    try:
        outer_logger.info('Serving on {}:{}'.format(host, port))
        loop.run_forever()
    except KeyboardInterrupt as e:
        outer_logger.info('Keyboard interrupted. Exit.')
    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
