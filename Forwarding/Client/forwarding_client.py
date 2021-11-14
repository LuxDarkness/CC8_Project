import os
import sys
import asyncio
import logging
import aiofiles as aiof
from contextlib import asynccontextmanager
sys.path.append('/home/michael/PycharmProjects/CC8_Project/')
from Model.communication_standards import Correct_Answer

script_dir = os.path.dirname(__file__)
formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')


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


class ForwardingClient:
    def __init__(self, msg, to, server_ip, make_chunks=False, chunk_start=0):
        self.time_wait_limit = 90
        self.to = to
        self.server_ip = server_ip
        self.port = 1981
        self.chunk_size = 730
        self.forward_message = msg
        self.make_chunks = make_chunks
        self.chunk_start = chunk_start
        self.storage = '../Storage/'

    async def start_sending(self):
        await self.send_message()

    async def send_message(self):
        logger = setup_logger(self.to, '../Logs/forwarding_client_to_{}'.format(self.to))
        awaited_time = 0
        while True:
            if awaited_time >= self.time_wait_limit:
                logger.error('Could not establish a connection with: {}'.format(self.to))
                break
            pre_connection = asyncio.open_connection(self.server_ip, self.port)
            try:
                reader, writer = await asyncio.wait_for(pre_connection, timeout=10)
            except asyncio.TimeoutError:
                awaited_time += 10
                logger.info('Could not connect to node: {}, Timeout, retrying.'.format(self.to))
                await asyncio.sleep(10)
                continue
            except Exception as conn_error:
                awaited_time += 10
                logger.error('Could not connect to node: {}, err: {}, retrying.'.format(self.to, conn_error))
                await asyncio.sleep(10)
                continue
            await self.communicate_with_server(writer, logger)
            break

    async def communicate_with_server(self, writer, logger):
        logger.info('Message to process: \n{}.'.format(self.forward_message))
        if self.make_chunks:
            message_lines = self.forward_message.split('\n')
            filename = message_lines[2][5:].strip()
            user_from = message_lines[0][5:].strip()
            user_to = message_lines[1][3:].strip()
            full_route = self.storage + filename
            size = os.path.getsize(full_route)
            async with aiof.open(full_route, mode='rb') as f:
                pos = 1
                async with self.file_chunks(f, self.chunk_size) as chunks:
                    async for chunk in chunks:
                        use_chunk = chunk.hex()
                        send_msg = Correct_Answer.format(user_from, user_to, filename, use_chunk, pos, size)
                        try:
                            writer.write(send_msg.encode())
                            await writer.drain()
                            logger.info('Chunk {} sent to {}.'.format(pos, logger.name))
                            logger.info('Message sent to {}: {}'.format(logger.name, send_msg))
                            pos += 1
                        except Exception as e:
                            logger.error('Could not send message to {}, chunk {} received error: {}.'.format(
                                logger.name, pos, e))
        else:
            awaited_time = 0
            while True:
                if awaited_time >= self.time_wait_limit:
                    logger.error('Could not send message to {}, retry from start.'.format(logger.name))
                    break
                try:
                    writer.write(self.forward_message.encode())
                    await writer.drain()
                    logger.info('Message successfully forwarded to {}.'.format(logger.name))
                    break
                except Exception as e:
                    logger.warning('Could not send message to {}, received error: {}.'.format(logger.name, e))
                    await asyncio.sleep(5)
                    awaited_time += 5

    @asynccontextmanager
    async def file_chunks(self, f, chunk_size):
        try:
            async def gen():
                b = await f.read(chunk_size)
                while b:
                    yield b
                    b = await f.read(chunk_size)
            yield gen()
        finally:
            f.close()
