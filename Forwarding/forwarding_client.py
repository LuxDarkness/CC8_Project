# import tqdm
import os
import asyncio
import aiofiles as aiof
from contextlib import asynccontextmanager
from communication_standards import Request

# def dv_test():
#     path = nx.single_source_bellman_ford_path(net, me_node, weight="weight")
#     length = nx.single_source_bellman_ford_path_length(net, me_node, weight="weight")


@asynccontextmanager
async def file_chunks(f, chunk_size):
    try:
        async def gen():
            b = await f.read(chunk_size)
            while b:
                yield b
                b = await f.read(chunk_size)
        yield gen()
    finally:
        f.close()

    async with aiof.open(filename, mode='rb') as f:
        pos = 0
        async with file_chunks(f, og_chunk_size) as chunks:
            async for chunk in chunks:
                writer.write(chunk.encode())
                await writer.drain()
                print(f"Writing to another one at position: {pos}")
                pos += len(chunk)


class ForwardingClient:
    def __init__(self):
        self.my_name = "Lux"
        self.to = "Otro"
        self.destiny_server = 'localhost'
        self.port = 1981

        self.base_filename = "imagen_prueba_1.jpg"
        self.filename = "./Storage/" + self.base_filename
        self.file_size = os.path.getsize(self.filename)

        self.req_msg = Request.format(self.my_name, self.to, self.base_filename, self.file_size)

    @staticmethod
    async def check_wrong_answer(data):
        msg_info = data.split("\n")
        if not (msg_info[0].__contains__("From:")) and msg_info[0][5:].strip():
            return False
        if not (msg_info[1].__contains__("To:")) and msg_info[1][3:].strip():
            return False
        if not (msg_info[2].__contains__("Msg:")) and msg_info[2][4:].strip():
            return False
        if not (msg_info[3].__contains__("EOF")):
            return False
        return True, False

    @staticmethod
    async def check_correct_answer(data):
        msg_info = data.split("\n")
        if not (msg_info[0].__contains__("From:")) and msg_info[0][5:].strip():
            return False
        if not (msg_info[1].__contains__("To:")) and msg_info[1][3:].strip():
            return False
        if not (msg_info[2].__contains__("Name:")) and msg_info[2][5:].strip():
            return False
        if not (msg_info[3].__contains__("Data:")) and msg_info[3][5:].strip():
            return False
        if not (msg_info[4].__contains__("Frag:")) and msg_info[4][5:].strip():
            return False
        if not (msg_info[5].__contains__("Size:")) and msg_info[5][5:].strip():
            return False
        if not (msg_info[6].__contains__("EOF")):
            return False
        return True, True

    async def check_format(self, data):
        if data.split("\n").__len__() == 7:
            return await self.check_correct_answer(data)
        elif data.split("\n").__len__() == 4:
            return await self.check_wrong_answer(data)
        return False, False

    @staticmethod
    async def collect_data(data):
        msg_info = data.split("\n")
        print(msg_info[3][5:].strip())

    async def receive_assemble(self, data):
        msg_format_correct, correct_msg_received = await self.check_format(data)
        if msg_format_correct and correct_msg_received:
            await self.collect_data(data)
        return True

    async def tcp_echo_client(self):
        reader, writer = await asyncio.open_connection(self.destiny_server, self.port)

        print(f'Send: {self.req_msg!r}')
        writer.write(self.req_msg.encode())
        await writer.drain()

        keep_waiting = True
        while keep_waiting:
            data = await reader.read(1024)
            keep_waiting = await self.receive_assemble(data)
        # print(f'Received: {data.decode()!r}')

        print('Close the connection')
        writer.close()
        await writer.wait_closed()


client = ForwardingClient()
asyncio.run(client.tcp_echo_client())

#  progress = tqdm.tqdm(range(file_size), f"Sending {filename}", unit="B", unit_scale=True, unit_divisor=BUFFER_SIZE)
