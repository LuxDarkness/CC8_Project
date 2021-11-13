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

max_message_wait_time = 30


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


async def parse_request(msg_lines, logger):
    logger.info('Trying to parse a REQUEST from {}.'.format(logger.name))
    if 'From:' not in msg_lines[0] or not msg_lines[0][5:].strip():
        # Not correct FROM
        logger.error('Incorrect REQUEST, bad FROM received from {}, received: {}.'.format(logger.name, msg_lines[0]))
        return 400
    if 'To:' not in msg_lines[1] or not msg_lines[1][3:].strip():
        # Not correct TO
        logger.error('Incorrect REQUEST, bad TO received from {}, received: {}.'.format(logger.name, msg_lines[1]))
        return 401
    if 'Name:' not in msg_lines[2] or not msg_lines[2][5:].strip():
        # Not correct NAME
        logger.error('Incorrect REQUEST, bad NAME received from {}, received: {}.'.format(logger.name, msg_lines[2]))
        return 402
    if 'Size:' not in msg_lines[3] or not msg_lines[3][5:].strip():
        # Not correct SIZE
        logger.error('Incorrect REQUEST, bad SIZE received from {}, received: {}.'.format(logger.name, msg_lines[3]))
        return 403
    if 'EOF' not in msg_lines[4]:
        # Not correct EOF
        logger.error('Incorrect REQUEST, bad EOF received from {}, received: {}.'.format(logger.name, msg_lines[4]))
        return 405
    # Correct REQUEST
    logger.info('Correct REQUEST received from {}.'.format(logger.name))
    return 100


async def parse_answer(msg_lines, logger):
    logger.info('Trying to parse CORRECT ANSWER from {}.'.format(logger.name))
    if 'From:' not in msg_lines[0] or not msg_lines[0][5:].strip():
        # Not correct FROM
        logger.error('Incorrect ANSWER, bad FROM received from {}, received: {}.'.format(logger.name, msg_lines[0]))
        return 400
    if 'To:' not in msg_lines[1] or not msg_lines[1][3:].strip():
        # Not correct TO
        logger.error('Incorrect ANSWER, bad TO received from {}, received: {}.'.format(logger.name, msg_lines[1]))
        return 401
    if 'Name:' not in msg_lines[2] or not msg_lines[2][5:].strip():
        # Not correct NAME
        logger.error('Incorrect ANSWER, bad NAME received from {}, received: {}.'.format(logger.name, msg_lines[2]))
        return 402
    if 'Data:' not in msg_lines[3] or not msg_lines[3][5:].strip():
        # Not correct DATA
        logger.error('Incorrect ANSWER, bad DATA received from {}, received: {}.'.format(logger.name, msg_lines[3]))
        return 406
    if 'Frag:' not in msg_lines[4] or not msg_lines[4][5:].strip():
        # Not correct FRAG
        logger.error('Incorrect ANSWER, bad FRAG received from {}, received: {}.'.format(logger.name, msg_lines[4]))
        return 407
    if 'Size:' not in msg_lines[5] or not msg_lines[5][5:].strip():
        # Not correct SIZE
        logger.error('Incorrect ANSWER, bad SIZE received from {}, received: {}.'.format(logger.name, msg_lines[5]))
        return 403
    if 'EOF' not in msg_lines[6]:
        # Not correct EOF
        logger.error('Incorrect ANSWER, bad EOF received from {}, received: {}.'.format(logger.name, msg_lines[6]))
        return 405
    # Correct CORRECT ANSWER received
    logger.info('Correct CORRECT ANSWER received from {}.'.format(logger.name))
    return 101


async def parse_error(msg_lines, logger):
    logger.info('Trying to parse ERROR ANSWER from {}.'.format(logger.name))
    print(msg_lines)


async def parse_msg(msg, logger):
    msg = msg.decode()
    msg_lines = msg.split('\n')
    if len(msg_lines) == 5:
        code = await parse_request(msg_lines, logger)
    elif len(msg_lines) == 7:
        code = await parse_answer(msg_lines, logger)
    elif len(msg_lines) == 4:
        code = await parse_error(msg_lines, logger)
    else:
        logger.error('Format for message from {} not found, message: {}.'.format(logger.name, msg))
        code = 404
    return code


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
    outer_logger.info('Successfully connected to client: {}'.format(logger.name))
    fails_counter = 0

    while True:
        try:
            msg = asyncio.wait_for(reader.read(1024), timeout=max_message_wait_time)
        except asyncio.TimeoutError:
            fails_counter += 1
            logger.warning('Timed out, no message received for {} seconds from {}.'.format(
                fails_counter * max_message_wait_time, logger.name))
            continue
        except Exception as conn_error:
            fails_counter += 1
            logger.error('Connection with {} damaged, error: {}'.format(logger.name, conn_error))
            continue

        code = await parse_msg(msg, logger)


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
