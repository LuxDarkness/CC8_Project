import tkinter as tk
from tkinter import ttk
import time
import os
import sys
import asyncio
import logging
import networkx as nx
sys.path.append('/home/michael/PycharmProjects/CC8_Project/')
from Client.disposable_client import DisposableClient
from Model.communication_standards import Request

script_dir = os.path.dirname(__file__)
formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s:%(message)s')
init_route_file = os.path.join(script_dir, '../Routing_Info')
clients_file_route = os.path.join(script_dir, '../Servers')
files_file_route = os.path.join(script_dir, '../available_to_ask_files')
routing_file_route = os.path.join(script_dir, '../Routing_Updates')


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


def get_net(logger):
    empty_graph = os.stat(routing_file_route).st_size == 0
    while empty_graph:
        time.sleep(1)
        empty_graph = os.stat(routing_file_route).st_size == 0
    logger.info('DV graph modified, update!')
    return nx.read_gpickle(routing_file_route)


def fill_to_combobox(combobox):
    combo_values = []
    for node in net.nodes():
        if my_name not in node:
            combo_values.append(node)
    combobox['values'] = combo_values
    print('Targets list updates!')


def update_to_combo(combobox, logger):
    global net
    net = get_net(logger)
    fill_to_combobox(combobox)


def fill_file_combobox(combobox):
    combo_values = []
    with open(files_file_route, 'r', encoding='UTF-8') as file:
        while line := file.readline().strip():
            combo_values.append(line)
    combobox['values'] = combo_values
    print('File list updated!')


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
    # loop.close()


if __name__ == '__main__':
    main_logger = setup_logger('main', '../Logs/app_gui_log')
    my_name = get_name()
    net = get_net(main_logger)

    window = tk.Tk()
    window.title('CC8 Project')

    to_string = tk.StringVar()
    filename_string = tk.StringVar()
    file_size_string = tk.StringVar()

    main_title = tk.Label(text='App')
    file_frame = tk.Frame()
    file_name_label = tk.Label(file_frame, text='Select file to request: ')
    file_name_combobox = ttk.Combobox(file_frame, textvariable=filename_string)
    file_name_combobox['state'] = 'readonly'
    to_frame = tk.Frame()
    to_label = tk.Label(to_frame, text='Select the person to request it from: ')
    to_combobox = ttk.Combobox(to_frame, textvariable=to_string)
    to_combobox['state'] = 'readonly'
    send_request_button = tk.Button(text='Send request', width=25, command=handle_send_click)
    updates_frame = tk.Frame()
    update_nodes_button = tk.Button(updates_frame, text='Update to send',
                                    command=lambda: update_to_combo(to_combobox, main_logger))
    update_files_button = tk.Button(updates_frame, text='Update files',
                                    command=lambda: fill_file_combobox(file_name_combobox))

    fill_to_combobox(to_combobox)
    fill_file_combobox(file_name_combobox)

    main_title.pack(fill='x', side='top')
    file_frame.pack(fill='x')
    file_name_label.pack(side='left')
    file_name_combobox.pack(side='right')
    to_frame.pack(fill='x')
    to_label.pack(side='left')
    to_combobox.pack(side='right')
    send_request_button.pack(fill='x')
    updates_frame.pack(fill='x')
    update_nodes_button.pack(side='left', expand=True)
    update_files_button.pack(side='right', expand=True)

    window.mainloop()
