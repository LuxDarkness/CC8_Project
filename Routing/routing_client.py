import os
import asyncio
import time
import networkx as nx
import logging
import re
from socket import error as socket_error
import errno
from asyncinotify import Inotify, Mask
from communication_standards import Routing_Client_Start
from communication_standards import Routing_Client_Send_Update
from communication_standards import Routing_Client_Keep_Alive
from communication_standards import Routing_Route_Msg


formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')

init_route_file = '../Routing_Info'
routing_file_route = '../Routing_Updates'
servers_file_route = '../Servers'
lock = asyncio.Lock()
t_interval = 10
connection_retry_limit = 100
wait_welcome_retry_limit = 100
try_to_say_hi_limit = 10
failed_updates_limit = 10
send_keep_alive_time = 20
to_update_neighbors = []


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


async def get_net(logger):
    async with lock:
        empty_graph = os.stat(routing_file_route).st_size == 0
        while empty_graph:
            await asyncio.sleep(1)
            empty_graph = os.stat(routing_file_route).st_size == 0
        logger.info('DV graph modified, update!')
        for s in servers_dict.keys():
            if s not in to_update_neighbors:
                to_update_neighbors.append(s)
        return nx.read_gpickle(routing_file_route)


def get_servers():
    temp_dict = {}
    with open(servers_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            node_info = line.split(":")
            temp_dict[node_info[0]] = node_info[1]
    return temp_dict


async def watch_net(logger):
    global net
    while True:
        with Inotify() as inotify:
            inotify.add_watch(routing_file_route, Mask.MODIFY)
            async for _ in inotify:
                net = await get_net(logger)


async def load_q():
    for server in servers_dict.keys():
        await servers_q.put(server)


async def parse_welcome_msg(msg):
    msg = msg.decode()
    msg = re.sub(r'\n+', '\n', msg).strip()
    if msg == '':
        # Connection lost
        return 420
    msg_lines = msg.split('\n')
    if len(msg_lines) > 2:
        # Unknown format
        return 415
    if 'From:' not in msg_lines[0] or not msg_lines[0][5:].strip():
        # Bad formatted WELCOME, bad FROM
        return 400
    is_from = msg_lines[0][5:].strip()
    if is_from not in servers_dict.keys():
        # Who are you?
        return 410
    if "Type:WELCOME" not in msg_lines[1]:
        # Bad formatted WELCOME, bad Type
        return 405
    return 100


async def log_msg_error(msg, code, logger):
    msg = msg.decode()
    if code == 400:
        first_error = 'From incorrectly formatted'
    elif code == 405:
        first_error = 'Type incorrectly formatted'
    elif code == 410:
        first_error = 'Sender not in neighbors list'
    elif code == 420:
        first_error = 'Connection lost'
    else:
        first_error = 'Unknown format error'
    logger.error('{} is not a valid WELCOME message, first error detected: {}.'.format(msg, first_error))


async def format_changes_msg(logger):
    lengths = nx.single_source_bellman_ford_path_length(net, my_name, weight='weight')
    base_msg = Routing_Client_Send_Update.format(my_name, len(lengths) - 1)
    for server in lengths.keys():
        if not (my_name in server):
            base_msg += Routing_Route_Msg.format(server, lengths.get(server))
            base_msg += '\n'
    logger.info('DV message to {}:\n{}.'.format(logger.name, base_msg))
    return base_msg


async def connect_and_communicate():
    server = await servers_q.get()
    ip = servers_dict.get(server)
    connection_logger = setup_logger('Connection', '../Logs/routing_client_connection_logger', True)
    server_logger = setup_logger(server, '../Logs/routing_client_to_{}'.format(server))
    failed_conn_counter = 0
    while True:
        if failed_conn_counter == wait_welcome_retry_limit:
            connection_logger.error('Could not establish a connection with: {}'.format(server))
            break
        pre_connection = asyncio.open_connection(ip, port)
        try:
            reader, writer = await asyncio.wait_for(pre_connection, timeout=10)
        except asyncio.TimeoutError:
            failed_conn_counter += 1
            connection_logger.info('Could not connect to node: {}, Timeout, retrying.'.format(server))
            await asyncio.sleep(10)
            continue
        except Exception as conn_error:
            failed_conn_counter += 1
            connection_logger.error('Could not connect to node: {}, err: {}, retrying.'.format(server, conn_error))
            await asyncio.sleep(10)
            continue
        failed_conn_counter = 0
        await communicate_with_server(reader, writer, server_logger)


async def send_greeting(writer, logger):
    greeting = Routing_Client_Start.format(my_name)
    hi_failed_counter = 0
    while True:
        if hi_failed_counter == try_to_say_hi_limit:
            logger.error('Could not send HELLO message to {}.'.format(logger.name))
            return False
        try:
            writer.write(greeting.encode())
            await writer.drain()
            hi_failed_counter = 0
            logger.info('Correctly sent HELLO message to {}.'.format(logger.name))
            return True
        except Exception as write_greeting_fail:
            logger.warning('Could not send HELLO to {}, err: {}'.format(logger.name, write_greeting_fail))
            hi_failed_counter += 1


async def receive_welcome(reader, logger):
    fails_counter = 0
    msg = ''
    while True:
        if fails_counter == wait_welcome_retry_limit:
            logger.error('WELCOME message not received from: {}'.format(logger.name))
            return False
        try:
            msg = await asyncio.wait_for(reader.read(1024), timeout=t_interval)
            fails_counter = 0
            break
        except asyncio.TimeoutError:
            fails_counter += 1
            logger.warning('Welcome message from {} was not received, {} times.'.format(logger.name, fails_counter))
            continue
        except Exception as unexpected_error:
            fails_counter += 1
            logger.error('An unexpected error occurred while waiting for welcome message from {}, error: {}'
                         .format(logger.name, unexpected_error))
    welcome_received_code = await parse_welcome_msg(msg)
    if welcome_received_code == 100:
        logger.info('Connection successfully established with: {}, WELCOME received.'.format(logger.name))
        return True
    else:
        await log_msg_error(msg, welcome_received_code, logger)
        return False


async def communicate_with_server(reader, writer, logger):
    logger.info('{} connected to the server.'.format(logger.name))
    greeted = await send_greeting(writer, logger)
    if not greeted:
        return
    welcomed = await receive_welcome(reader, logger)
    if not welcomed:
        return
    last_sent_msg_counter = 0
    failed_update_messages = 0
    while True:
        if failed_update_messages == failed_updates_limit:
            logger.error('Could not send any update or ka messages to: {}.'.format(logger.name))
            return
        if logger.name in to_update_neighbors:
            net_changes_msg = await format_changes_msg(logger)
            try:
                writer.write(net_changes_msg.encode())
                await writer.drain()
                logger.info('DV update successfully sent to: {}.'.format(logger.name))
                to_update_neighbors.remove(logger.name)
                last_sent_msg_counter = 0
                failed_update_messages = 0
            except socket_error as s_error:
                if s_error.errno != errno.ECONNRESET:
                    logger.error('Server {} socket error: {}.'.format(logger.name, s_error))
                    return
                logger.error('Server {} disconnected, error: {}.'.format(logger.name, s_error))
                return
            except Exception as update_except:
                logger.warning('Could not send dv update message to: {}, error: {}.'.format(logger.name, update_except))
                failed_update_messages += 1
                continue
        await asyncio.sleep(1)
        last_sent_msg_counter += 1
        if last_sent_msg_counter == send_keep_alive_time:
            keep_alive_msg = Routing_Client_Keep_Alive.format(my_name)
            try:
                writer.write(keep_alive_msg.encode())
                await writer.drain()
                logger.info('Keep Alive message successfully sent to: {}.'.format(logger.name))
                last_sent_msg_counter = 0
                failed_update_messages = 0
            except socket_error as s_error:
                if s_error.errno != errno.ECONNRESET:
                    logger.error('Server {} socket error: {}.'.format(logger.name, s_error))
                    failed_update_messages += 1
                    continue
                logger.error('Server {} disconnected, error: {}.'.format(logger.name, s_error))
                to_update_neighbors.append(logger.name)
                return
            except Exception as keep_except:
                logger.warning('Could not send keep alive message to: {}, error: {}.'.format(logger.name, keep_except))
                failed_update_messages += 1


if __name__ == '__main__':
    net_logger = setup_logger('Net_logger', '../Logs/routing_client_net_logger')
    outer_logger = setup_logger('outer', '../Logs/routing_client_outer_logger', True)

    servers_q = asyncio.Queue()
    my_name = get_name()
    port = 9080
    servers_dict = get_servers()
    servers_quantity = len(servers_dict)
    net = get_first_net(net_logger)

    loop = asyncio.get_event_loop()
    coroutines = [connect_and_communicate() for _ in range(servers_quantity)]
    coroutines.insert(0, watch_net(net_logger))
    loop.run_until_complete(load_q())
    loop.run_until_complete(asyncio.wait(coroutines))

    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        outer_logger.info('Keyboard interrupted. Exit.')
    except Exception as error:
        outer_logger.error(error)
    loop.close()
