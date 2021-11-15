import tkinter as tk
from tkinter import ttk
import os
import sys
import asyncio
import logging
sys.path.append('/home/michael/PycharmProjects/CC8_Project/')
from Client.disposable_client import DisposableClient
from Model.communication_standards import Request

script_dir = os.path.dirname(__file__)
formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')
init_route_file = os.path.join(script_dir, '../Routing_Info')
clients_file_route = os.path.join(script_dir, '../Servers')
files_file_route = os.path.join(script_dir, '../available_to_ask_files')


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


def get_name():
    with open(init_route_file, 'r', encoding='UTF-8') as file:
        name_line = file.readline()
    return name_line[4:].strip()


def fill_to_combobox(combobox):
    combo_values = []
    with open(clients_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            node_info = line.split(":")
            node_name = node_info[0]
            combo_values.append(node_name)
    combobox['values'] = combo_values


def fill_file_combobox(combobox):
    combo_values = []
    with open(files_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            combo_values.append(line)
    combobox['values'] = combo_values


def handle_send_click():
    filename_info = file_name_combobox.get()
    to = to_combobox.get()

    if not filename_info:
        print('No file info selected...')
        return
    if not to:
        print('Not acceptable value selected...')
        return

    file_info = filename_info.split(':')
    filename = file_info[0]
    size = file_info[1]
    size = int(size)

    msg = Request.format(my_name, to, filename, size)
    client = DisposableClient(msg, my_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(client.send_message())
    loop.close()


if __name__ == '__main__':
    my_name = get_name()

    window = tk.Tk()

    to_string = tk.StringVar()
    filename_string = tk.StringVar()
    file_size_string = tk.StringVar()

    main_title = tk.Label(text='App')
    file_name_label = tk.Label(text='Select file to request: ')
    file_name_combobox = ttk.Combobox(window, textvariable=filename_string)
    file_name_combobox['state'] = 'readonly'
    to_label = tk.Label(text='Select the person to request it from: ')
    to_combobox = ttk.Combobox(window, textvariable=to_string)
    to_combobox['state'] = 'readonly'
    send_request_button = tk.Button(text='Send request', width=25, height=2, command=handle_send_click)

    fill_to_combobox(to_combobox)
    fill_file_combobox(file_name_combobox)

    main_title.pack(fill='X')
    file_name_label.pack()
    file_name_combobox.pack()
    to_label.pack()
    to_combobox.pack()
    send_request_button.pack()

    window.mainloop()
