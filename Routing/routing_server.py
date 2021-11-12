import asyncio
import networkx as nx
import logging
import os
import re
from communication_standards import Routing_Server_Greeting

formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')

clients_file_route = '../Servers'
routing_updates_route = '../Routing_Updates'
lock = asyncio.Lock()
host = "0.0.0.0"
port = 9080
nodes = []
edges = []
net = nx.Graph()
routes_dict = {}
clients_dict = {}
original_weight_dict = {}
me_node = None
t_interval = 30
u_interval = 90
send_greeting_errors_limit = 10


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


def get_clients():
    temp_dict = {}
    with open(clients_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            node_info = line.split(":")
            temp_dict[node_info[0]] = node_info[1]
    return temp_dict


def clean_updates():
    open(routing_updates_route, 'w').close()


async def update_routing():
    net_updated = False
    edges_list = []
    for from_node in routes_dict.keys():
        for to_node in routes_dict.get(from_node).keys():
            edges_list.append((from_node, to_node, routes_dict.get(from_node).get(to_node)))
    routes_dict.clear()
    for edge in edges_list:
        if net.has_edge(edge[0], edge[1]):
            weight = net.get_edge_data(edge[0], edge[1]).get('weight')
            if (int(edge[2]) < weight or int(edge[2]) >= 99) and me_node not in edge[0] and me_node not in edge[1]:
                net[edge[0]][edge[1]]['weight'] = int(edge[2]) if int(edge[2]) <= 99 else 99
                net_updated = True
        else:
            net.add_edge(edge[0], edge[1], weight=int(edge[2]) if int(edge[2]) <= 99 else 99)
            net_updated = True
    if net_updated:
        await update_routing_file()


async def update_routing_file():
    async with lock:
        with open(routing_updates_route, 'r+') as file:
            file.truncate(0)
            nx.write_gpickle(net, routing_updates_route)
            file.close()


def parse_line(line):
    if "Me:" in line:
        return line[3:].strip(), True
    elif "Nodes:" in line:
        nodes.extend(line[7:].split(","))
        return "", False
    elif "Costs:" in line:
        return "", False
    else:
        items = line.split(",")
        edges.append((items[0], items[1], int(items[2])))
        return "", False


def create_network():
    me_temp_node = None
    with open('../Routing_Info', 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            me, is_valid = parse_line(line)
            if is_valid:
                me_temp_node = me
    net.add_nodes_from(nodes)
    net.add_weighted_edges_from(edges)
    nx.write_gpickle(net, routing_updates_route)
    return me_temp_node


async def parse_start(lines):
    if not("From:" in lines[0]) or not(lines[0][5:].strip()):
        # Bad formatted hello msg
        return 400
    msg_from = lines[0][5:].strip()
    if not(msg_from in nodes):
        # Who are you?
        return 405
    # HELLO msg correctly processed
    return 100


async def parse_ka(lines):
    if not("From:" in lines[0]) or not(lines[0][5:].strip()):
        # Bad formatted keep alive msg
        return 410
    msg_from = lines[0][5:].strip()
    if not(msg_from in nodes):
        # Who are you?
        return 405
    # KeepAlive msg correctly processed
    return 110


async def parse_double(lines):
    if "Type:HELLO" in lines[1]:
        return await parse_start(lines)
    if "Type:KeepAlive" in lines[1]:
        return await parse_ka(lines)
    # Unknown format msg
    return 430


async def parse_dv_info(lines):
    if not("From:" in lines[0]) or not(lines[0][5:].strip()):
        # Not correct DV request
        return 415
    if not("Type:DV" in lines[1]):
        # Not correct DV request
        return 415
    if not("Len:" in lines[2]) or not(lines[2][4:].strip()):
        # Not correct DV request
        return 415
    msg_from = lines[0][5:].strip()
    if not (msg_from in nodes):
        # Who are you?
        return 405
    lines_len = int(lines[2][4:].strip())
    routes_dict[msg_from] = {}
    for i in range(lines_len):
        line_info = lines[i+3].split(":")
        if len(line_info) == 2:
            if len(line_info[0]) == 0 or len(line_info[1]) == 0:
                # Bad formatted DV info
                routes_dict.get(msg_from).clear()
                return 420
            routes_dict.get(msg_from)[line_info[0].strip()] = line_info[1].strip()
        else:
            # Bad formatted DV info
            routes_dict.get(msg_from).clear()
            return 420
    # DV msg correctly processed
    return 105


async def parse_request(msg):
    msg_lines = msg.decode()
    msg_lines = re.sub(r'\n+', '\n', msg_lines).strip()
    if msg_lines == '':
        # Lost connection
        return 425
    msg_lines = msg_lines.split("\n")
    if len(msg_lines) == 2:
        return await parse_double(msg_lines)
    else:
        return await parse_dv_info(msg_lines)


async def send_greeting(writer, logger):
    fails_counter = 0
    wel_msg = Routing_Server_Greeting.format(me_node)
    while True:
        if fails_counter == send_greeting_errors_limit:
            logger.error('Could not send greeting to: {}, disconnecting.'.format(logger.name))
            return False
        try:
            writer.write(wel_msg.encode())
            await writer.drain()
            # logger.info('Message sent to {}: {}'.format(logger.name, wel_msg))
            return True
        except Exception as send_error:
            fails_counter += 1
            logger.warning(
                'Could not send greeting to: {}, times: {}, error: {}'.format(logger.name, fails_counter, send_error))


async def wrong_format_message(code, msg, logger):
    msg = msg.decode()
    finish = False
    if code == 425:
        first_error = 'Empty message received, maybe connection has been lost with client {}'.format(logger.name)
        finish = True
    elif code == 430:
        first_error = 'Unknown message format received'
    elif code == 400:
        first_error = 'HELLO message not correctly formatted'
    elif code == 405:
        first_error = 'Message received from unknown client'
    elif code == 410:
        first_error = 'Keep Alive message not correctly formatted'
    elif code == 415:
        first_error = 'Expected DV format message'
    elif code == 420:
        first_error = 'DV information not correctly formatted'
    else:
        first_error = 'Unknown message format'
    logger.warning('Message received: {}, with first error: {}.'.format(msg, first_error))
    return finish


async def update_non_responding_client(logger):
    if logger.name not in original_weight_dict.keys():
        og_weight = net.get_edge_data(me_node, logger.name).get('weight')
        original_weight_dict[logger.name] = og_weight
    try:
        this_edge = [(me_node, logger.name, 99)]
        net.remove_node(logger.name)
        net.add_weighted_edges_from(this_edge)
        await update_routing_file()
        logger.info('{} did not respond, updating weight to 99'.format(logger.name))
    except KeyError:
        pass


async def serve_client_cb(client_reader, client_writer):
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

    if client_logger.name in original_weight_dict.keys():
        net[me_node][client_logger.name]['weight'] = int(original_weight_dict.get(client_logger.name))

    asyncio.ensure_future(client_task(client_reader, client_writer, client_logger))


async def client_task(reader, writer, logger):
    outer_logger.info('Successfully connected to client: {}'.format(logger.name))
    fails_counter = 0
    fails_limit = int(u_interval / t_interval) + 1

    while True:
        if fails_counter == fails_limit:
            logger.error('Connection lost to client: {}, closing.'.format(logger.name))
            await update_non_responding_client(logger)
            writer.close()
            return

        try:
            msg = await asyncio.wait_for(reader.read(1024), timeout=t_interval)
            fails_counter = 0
        except asyncio.TimeoutError:
            fails_counter += 1
            logger.warning('Timed out, no message received for {} seconds from {}.'.format(
                fails_counter * t_interval, logger.name))
            continue
        except Exception as conn_error:
            fails_counter += 1
            logger.error('Connection with {} damaged, error: {}'.format(logger.name, conn_error))
            continue

        # logger.info('Received: {}'.format(msg.decode()))
        code = await parse_request(msg)

        if code == 100:
            logger.info('Correctly received HELLO message from {}.'.format(logger.name))
            msg_sent = await send_greeting(writer, logger)
            if not msg_sent:
                logger.error('Disconnecting client: {}.'.format(logger.name))
                await update_non_responding_client(logger)
                writer.close()
                return
        elif code == 105:
            await update_routing()
            logger.info('Correctly updated DV information from {} in server.'.format(logger.name))
        elif code == 110:
            logger.info('Correctly received keep alive message from {}.'.format(logger.name))
        else:
            finish_conn = await wrong_format_message(code, msg, logger)
            if finish_conn:
                await asyncio.sleep(10)
                fails_counter += 1


if __name__ == '__main__':
    outer_logger = setup_logger('outer_logger', '../Logs/routing_server_outer')
    clean_updates()
    me_node = create_network()
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
