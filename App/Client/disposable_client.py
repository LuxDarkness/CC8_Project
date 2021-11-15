import os
import asyncio
import logging


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


class DisposableClient:
    def __init__(self, msg, to):
        self.time_wait_limit = 30
        self.to = to
        self.server_ip = '127.0.0.1'
        self.port = 1981
        self.forward_message = msg

    async def send_message(self):
        logger = setup_logger(self.to, '../../Logs/app_client_to_{}'.format(self.to))
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
        awaited_time = 0
        while True:
            if awaited_time >= self.time_wait_limit:
                logger.error('Could not send message to {}, retry from start.'.format(logger.name))
                break
            try:
                writer.write(self.forward_message.encode())
                await writer.drain()
                logger.info('Message successfully sent to {}.'.format(logger.name))
                break
            except Exception as e:
                logger.warning('Could not send message to {}, received error: {}.'.format(logger.name, e))
                await asyncio.sleep(5)
                awaited_time += 5
