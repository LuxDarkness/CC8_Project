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
connected_clients = []
not_connected_for = {}
net = nx.Graph()
routes_dict = {}
clients_dict = {}
original_weight_dict = {}
me_node = None
t_interval = 30
u_interval = 90
watcher_interval = 15


def setup_logger(name, log_file, clean_file=True, level=logging.DEBUG):
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
        await draw_graph_update()


async def draw_graph_update():
    print('----------------Graph-----------------')
    for u, v, w in net.edges(data=True):
        print(u, v, w)
    print('----------------Graph-----------------')


async def update_routing_file():
    async with lock:
        with open(routing_updates_route, 'r+') as file:
            file.truncate(0)
            nx.write_gpickle(net, routing_updates_route)
            file.close()


async def connected_watcher():
    while True:
        await asyncio.sleep(watcher_interval)
        for act_client in not_connected_for.keys():
            not_connected_for[act_client] = not_connected_for[act_client] + watcher_interval
            not_connected_time = not_connected_for.get(act_client)
            if not_connected_time >= u_interval:
                logger = setup_logger(act_client, '../Logs/routing_server_to_{}'.format(act_client))
                logger.info('Client {} did not connect in the last {} seconds.'.format(logger.name, not_connected_time))
                await update_non_responding_client(logger)


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
    if "From:" not in lines[0] or not lines[0][5:].strip():
        # Bad formatted hello msg
        return 400
    msg_from = lines[0][5:].strip()
    if msg_from not in nodes:
        # Who are you?
        return 405
    # HELLO msg correctly processed
    return 100


async def parse_ka(lines):
    if "From:" not in lines[0] or not lines[0][5:].strip():
        # Bad formatted keep alive msg
        return 410
    msg_from = lines[0][5:].strip()
    if msg_from not in nodes:
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


async def parse_dv_info(lines, msg, logger):
    if "From:" not in lines[0] or not lines[0][5:].strip():
        # Not correct DV request
        return 415
    if "Type:DV" not in lines[1]:
        # Not correct DV request
        return 415
    if "Len:" not in lines[2] or not lines[2][4:].strip():
        # Not correct DV request
        return 415
    msg_from = lines[0][5:].strip()
    if msg_from not in nodes:
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
    logger.info('From {} received DV: \n{}'.format(logger.name, msg))
    return 105


async def parse_request(msg, logger):
    msg = msg.decode()
    msg_lines = re.sub(r'\n+', '\n', msg).strip()
    if msg_lines == '':
        # Lost connection
        return 425
    msg_lines = msg_lines.split("\n")
    if len(msg_lines) == 2:
        return await parse_double(msg_lines)
    else:
        return await parse_dv_info(msg_lines, msg, logger)


async def send_greeting(writer, logger):
    fails_counter = 0
    retry_time = 5
    wel_msg = Routing_Server_Greeting.format(me_node)
    send_greeting_errors_limit = int(u_interval/retry_time)
    while True:
        if fails_counter == send_greeting_errors_limit:
            logger.error('Could not send greeting to: {}, disconnecting.'.format(logger.name))
            return False
        try:
            writer.write(wel_msg.encode())
            await writer.drain()
            logger.info('WELCOME Message sent to {}.'.format(logger.name))
            return True
        except Exception as send_error:
            fails_counter += 1
            logger.warning(
                'Could not send greeting to: {}, times: {}, error: {}'.format(logger.name, fails_counter, send_error))
            await asyncio.sleep(retry_time)


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
    logger.warning('Message received: {}, from {} with first error: {}.'.format(msg, logger.name, first_error))
    return finish


async def update_non_responding_client(logger):
    if logger.name not in original_weight_dict.keys():
        og_weight = net.get_edge_data(me_node, logger.name).get('weight')
        original_weight_dict[logger.name] = og_weight
    try:
        if net.get_edge_data(me_node, logger.name).get('weight') < 99:
            this_edge = [(me_node, logger.name, 99)]
            net.remove_node(logger.name)
            net.add_weighted_edges_from(this_edge)
            await update_routing_file()
            await draw_graph_update()
            logger.info('{} did not respond, updating weight to 99'.format(logger.name))
    except KeyError:
        logger.error('Tried to remove non existent node: {}.'.format(logger.name))


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

    not_connected_for[client_name] = 0

    if client_name in connected_clients:
        all_tasks = asyncio.all_tasks()
        for task in all_tasks:
            if task.get_name() == client_name:
                if not task.cancelled():
                    task.cancel()
        connected_clients.remove(client_name)

    connected_clients.append(client_name)
    client_logger = setup_logger(client_name, '../Logs/routing_server_to_{}'.format(client_name))
    client_logger.info('Client connected: {}'.format(client_id))

    if client_logger.name in original_weight_dict.keys():
        if net[me_node][client_logger.name]['weight'] == 99:
            net[me_node][client_logger.name]['weight'] = int(original_weight_dict.get(client_logger.name))
            await update_routing_file()
            await draw_graph_update()

    asyncio.create_task(client_task(client_reader, client_writer, client_logger))


async def client_task(reader, writer, logger):
    outer_logger.info('Successfully connected to client: {}'.format(logger.name))
    asyncio.current_task().set_name(logger.name)
    waited_time = 0

    while True:
        if waited_time >= u_interval:
            logger.error('Connection lost to client: {}, closing.'.format(logger.name))
            await update_non_responding_client(logger)
            writer.close()
            connected_clients.remove(logger.name)
            return

        try:
            msg = await asyncio.wait_for(reader.read(1024), timeout=t_interval)
        except asyncio.TimeoutError:
            waited_time += t_interval
            logger.warning('Timed out, no message received for {} seconds from {}.'.format(
                waited_time, logger.name))
            continue
        except Exception as conn_error:
            logger.error('Connection with {} damaged, error: {}'.format(logger.name, conn_error))
            await asyncio.sleep(5)
            waited_time += 5
            continue

        # logger.info('Received: {}'.format(msg.decode()))
        code = await parse_request(msg, logger)

        if code == 100:
            waited_time = 0
            not_connected_for[logger.name] = 0
            logger.info('Correctly received HELLO message from {}.'.format(logger.name))
            msg_sent = await send_greeting(writer, logger)
            if not msg_sent:
                logger.error('Disconnecting client: {}.'.format(logger.name))
                await update_non_responding_client(logger)
                writer.close()
                connected_clients.remove(logger.name)
                return
        elif code == 105:
            not_connected_for[logger.name] = 0
            waited_time = 0
            await update_routing()
            logger.info('Correctly updated DV information from {} in server.'.format(logger.name))
        elif code == 110:
            not_connected_for[logger.name] = 0
            waited_time = 0
            logger.info('Correctly received keep alive message from {}.'.format(logger.name))
        else:
            finish_conn = await wrong_format_message(code, msg, logger)
            if finish_conn:
                not_connected_for[logger.name] = 0
                await asyncio.sleep(10)
                waited_time += 10


if __name__ == '__main__':
    outer_logger = setup_logger('outer_logger', '../Logs/routing_server_outer')
    clean_updates()
    me_node = create_network()
    clients_dict = get_clients()
    for client in clients_dict.keys():
        not_connected_for[client] = 0
    loop = asyncio.get_event_loop()
    check_not_connected = connected_watcher()
    server_core = asyncio.start_server(serve_client_cb,
                                       host=host,
                                       port=port,
                                       loop=loop)
    coroutines = [check_not_connected, server_core]
    loop.run_until_complete(asyncio.wait(coroutines))

    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        outer_logger.info('Keyboard interrupted. Exit.')
    loop.close()
