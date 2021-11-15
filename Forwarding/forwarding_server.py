import asyncio
import sys
import logging
import os
import re
import networkx as nx
import time
from Client.forwarding_client import ForwardingClient
from asyncinotify import Inotify, Mask
sys.path.append('/home/michael/PycharmProjects/CC8_Project/')
from Model.communication_standards import Error_Answer

script_dir = os.path.dirname(__file__)
formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')

og_chunk_size = 1024
host = "0.0.0.0"
port = 1981
clients_dict = {}
receive_file = {}
lock = asyncio.Lock()
storage_route = os.path.join(script_dir, './../Storage/')
received_route = os.path.join(script_dir, './../Received/')
clients_file_route = os.path.join(script_dir, '../Servers')
init_route_file = os.path.join(script_dir, '../Routing_Info')
routing_file_route = os.path.join(script_dir, '../Routing_Updates')

correct_codes_list = [100, 101, 102]
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


def setup_logger(name, log_file, clean_file=True, level=logging.DEBUG):
    file_dir = os.path.join(script_dir, log_file)
    file_dir = os.path.abspath(os.path.realpath(file_dir))
    if not os.path.exists(file_dir):
        os.mknod(file_dir)
    if clean_file:
        open(file_dir, 'w').close()

    handler = logging.FileHandler(file_dir)
    handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.addHandler(console_handler)

    return logger


async def get_net(logger):
    async with lock:
        empty_graph = os.stat(routing_file_route).st_size == 0
        while empty_graph:
            time.sleep(1)
            empty_graph = os.stat(routing_file_route).st_size == 0
        logger.info('DV graph modified, update!')
        return nx.read_gpickle(routing_file_route)


async def watch_net(logger):
    global net
    while True:
        with Inotify() as inotify:
            inotify.add_watch(routing_file_route, Mask.CLOSE_WRITE)
            async for _ in inotify:
                net = await get_net(logger)


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
    if 'From:' not in msg_lines[0] or not msg_lines[0][5:].strip():
        # Not correct FROM
        logger.error('Incorrect ERROR, bad FROM received from {}, received: {}.'.format(logger.name, msg_lines[0]))
        return 400
    if 'To:' not in msg_lines[1] or not msg_lines[1][3:].strip():
        # Not correct TO
        logger.error('Incorrect ERROR, bad TO received from {}, received: {}.'.format(logger.name, msg_lines[1]))
        return 401
    if 'Msg:' not in msg_lines[2] or not msg_lines[2][4:].strip():
        # Not correct MSG
        logger.error('Incorrect ERROR, bad MSG received from {}, received: {}.'.format(logger.name, msg_lines[2]))
        return 408
    if 'EOF' not in msg_lines[3]:
        # Not correct EOF
        logger.error('Incorrect ERROR, bad EOF received from {}, received: {}.'.format(logger.name, msg_lines[3]))
        return 405
    logger.info('Correct CORRECT ERROR received from {}.'.format(logger.name))
    return 102


async def parse_msg(msg, logger):
    msg = msg.decode()
    msg = re.sub(r'\n+', '\n', msg).strip()
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


async def get_error_message(code):
    if code == 400:
        return 'Bad FROM received, who are you supposed to be?'
    elif code == 401:
        return 'Bad TO received, who are we sending it to?'
    elif code == 402:
        return 'Incorrect NAME received, is this even a file?'
    elif code == 403:
        return 'Incorrect SIZE received, is this a number or something else?'
    elif code == 404:
        return 'I do not even know what kind of format this is supposed to be!'
    elif code == 405:
        return 'Wrong EOF line, how do you mess this up?'
    elif code == 406:
        return 'Incorrect DATA received, how am I supposed to receive it?'
    elif code == 407:
        return 'Bad FRAG received, not very good at counting are we.'
    elif code == 408:
        return 'Bad ERROR received, did you forget even this?'
    elif code == 409:
        return 'I do not have this file, did you mean to ask someone else?'
    elif code == 410:
        return 'I have the file, it is just of another size, maybe try again?'


async def create_file(file_received_data, filename):
    full_route = received_route + filename
    if not os.path.exists(full_route):
        os.mknod(full_route)
    actual_frag = 1
    last_frag = 0
    keep_writing = True
    for frag in file_received_data.keys():
        if int(frag) > last_frag:
            last_frag = int(frag)
    with open(full_route, 'wb') as file:
        while keep_writing:
            for frag in file_received_data.keys():
                if int(frag) == actual_frag:
                    str_frag = file_received_data.get(frag)
                    file.write(str_frag)
                    actual_frag += 1
                if actual_frag > last_frag:
                    keep_writing = False
        file.close()


async def forward_message(msg, code, logger):
    msg = msg.decode()
    msg = re.sub(r'\n+', '\n', msg).strip()
    print(msg)
    msg_lines = msg.split('\n')
    send_to = msg_lines[1][3:].strip()
    original_send_to = msg_lines[1][3:].strip()
    if my_name not in send_to:
        length, path = nx.single_source_bellman_ford(net, source=my_name, target=send_to, weight='weight')
    else:
        send_to = msg_lines[0][5:].strip()
        length, path = nx.single_source_bellman_ford(net, source=my_name, target=send_to, weight='weight')
    forward_to = path[1]
    if my_name in original_send_to:
        if code == 100:
            filename = msg_lines[2][5:].strip()
            full_route = storage_route + filename
            required_size = int(msg_lines[3][5:].strip())
            file_exists = os.path.isfile(full_route)
            real_size = 0
            if file_exists:
                real_size = os.path.getsize(full_route)
            if file_exists and real_size == required_size:
                client = ForwardingClient(msg, forward_to, clients_dict.get(forward_to), make_chunks=True)
                await client.start_sending()
                logger.info('Successfully answered request to: {}.'.format(forward_to))
            elif not file_exists:
                # File doesn't exists
                await forward_error(msg.encode(), 409, logger)
            elif not real_size == required_size:
                # Size doesn't matches
                await forward_error(msg.encode(), 410, logger)
        elif code == 101:
            filename = msg_lines[2][5:].strip()
            frag = msg_lines[4][5:].strip()
            data = msg_lines[3][5:].strip()
            file_size = int(msg_lines[5][5:].strip())
            data = bytes.fromhex(data)
            logger.info('The length of the data received is: {}.'.format(len(data)))
            if filename not in receive_file.keys():
                receive_file[filename] = {}
            receive_file.get(filename)[frag] = data
            logger.info('Correctly received {} data frag {}, from {}.'.format(filename, frag, logger.name))
            file_received_data = receive_file.get(filename)
            total_received = 0
            for chunk in file_received_data.keys():
                total_received += len(file_received_data.get(chunk))
                if total_received >= file_size:
                    await create_file(file_received_data, filename)
                    receive_file.get(filename).clear()
                    logger.info('Successfully created file received from {}.'.format(logger.name))
                    return True
            return False
        elif code == 102:
            logger.info('Received error message: \n{}.'.format(msg))
            return True
    else:
        client = ForwardingClient(msg, forward_to, clients_dict.get(forward_to))
        await client.start_sending()
        logger.info('Successfully forwarded message to: {}, message: \n{}.'.format(forward_to, msg))
        return True
    return False


async def forward_error(msg, code, logger):
    msg = msg.decode()
    msg = re.sub(r'\n+', '\n', msg).strip()
    msg_lines = msg.split('\n')
    user_from = msg_lines[0][5:].strip()
    try:
        length, path = nx.single_source_bellman_ford(net, source=my_name, target=user_from, weight='weight')
    except nx.NetworkXNoPath:
        length, path = nx.single_source_bellman_ford(net, source=my_name, target=logger.name, weight='weight')
    forward_to = path[1]
    error_message = await get_error_message(code)
    send_message = Error_Answer.format(my_name, user_from, error_message)
    client = ForwardingClient(send_message, forward_to, clients_dict.get(forward_to))
    await client.start_sending()
    logger.info('Successfully forwarded error message to: {}, message: \n{}.'.format(forward_to, msg))


def serve_client_cb(client_reader, client_writer):
    client_id = client_writer.get_extra_info('peername')
    client_ip = client_id[0]
    client_name = ''

    for temp_client_name in clients_dict.keys():
        if clients_dict.get(temp_client_name) == client_ip:
            client_name = temp_client_name

    if not client_name:
        if client_ip == '127.0.0.1':
            client_name = my_name
        else:
            outer_logger.error('Not found client IP tried to connect, ip: {}'.format(client_ip))
            return

    client_logger = setup_logger(client_name, '../Logs/routing_server_to_{}'.format(client_name))
    client_logger.info('Client connected: {}'.format(client_id))

    asyncio.create_task(client_task(client_reader, client_logger))


async def client_task(reader, logger):
    outer_logger.info('Successfully connected to client: {}'.format(logger.name))
    fails_counter = 0
    nulls_limit = 5
    nulls_counter = 0
    while True:
        if nulls_limit == nulls_counter or fails_counter == 10:
            logger.info('Closing loose connection with client {}.'.format(logger.name))
            break
        try:
            msg = await asyncio.wait_for(reader.readuntil(separator=b'EOF'), timeout=max_message_wait_time)
            if not msg.strip():
                logger.info('Received empty message from {}.'.format(logger.name))
                await asyncio.sleep(3)
                nulls_counter += 1
                continue
        except asyncio.TimeoutError:
            fails_counter += 1
            logger.warning('Timed out, no message received for {} seconds from {}.'.format(
                fails_counter * max_message_wait_time, logger.name))
            continue
        except Exception as conn_error:
            fails_counter += 1
            logger.error('Connection with {} damaged, error: {}'.format(logger.name, conn_error))
            continue

        fails_counter = 0
        code = await parse_msg(msg, logger)

        if code in correct_codes_list:
            # break_hundred = await forward_message(msg, code, logger)
            break_hundred = await asyncio.create_task(forward_message(msg, code, logger))
            if code == 100 or break_hundred:
                break
        else:
            # await forward_error(msg, code, logger)
            await asyncio.create_task(forward_error(msg, code, logger))
            break


if __name__ == '__main__':
    outer_logger = setup_logger('outer_logger', '../Logs/forwarding_server_outer')
    my_name = get_name()
    net = get_first_net(outer_logger)
    clients_dict = get_clients()

    watcher = watch_net(outer_logger)

    loop = asyncio.get_event_loop()
    server_core = asyncio.start_server(serve_client_cb,
                                       host=host,
                                       port=port,
                                       loop=loop)
    coroutines = [watcher, server_core]
    loop.run_until_complete(asyncio.wait(coroutines))

    try:
        outer_logger.info('Serving on {}:{}'.format(host, port))
        loop.run_forever()
    except KeyboardInterrupt as e:
        outer_logger.info('Keyboard interrupted. Exit.')
    # Close the server
    loop.close()
