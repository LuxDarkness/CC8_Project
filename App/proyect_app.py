import PySimpleGUI as Simple_gui
import os.path

# First the window layout in 2 columns

file_list_column = [
    [
        Simple_gui.Text("Image Folder"),
        Simple_gui.In(size=(25, 1), enable_events=True, key="-FOLDER-"),
        Simple_gui.FolderBrowse(),
    ],
    [
        Simple_gui.Listbox(
            values=[], enable_events=True, size=(40, 20), key="-FILE LIST-"
        )
    ],
]

# For now will only show the name of the file that was chosen
image_viewer_column = [
    [Simple_gui.Text("Choose an image from list on left:")],
    [Simple_gui.Text(size=(40, 1), key="-TOUT-")],
    [Simple_gui.Image(key="-IMAGE-")],
]

# ----- Full layout -----
layout = [
    [
        Simple_gui.Column(file_list_column),
        Simple_gui.VSeperator(),
        Simple_gui.Column(image_viewer_column),
    ]
]

window = Simple_gui.Window("Image Viewer", layout)

# Run the Event Loop
while True:
    event, values = window.read()
    if event == "Exit" or event == Simple_gui.WIN_CLOSED:
        break
    # Folder name was filled in, make a list of files in the folder
    if event == "../Storage/":
        folder = values["../Storage/"]
        try:
            # Get list of files in folder
            file_list = os.listdir(folder)
        except Exception:
            file_list = []

        file_names = [
            f
            for f in file_list
            if os.path.isfile(os.path.join(folder, f))
            and f.lower().endswith((".png", ".gif"))
        ]
        window["-FILE LIST-"].update(file_names)
    elif event == "-FILE LIST-":  # A file was chosen from the listbox
        try:
            filename = os.path.join(
                values["-FOLDER-"], values["-FILE LIST-"][0]
            )
            window["-TOUT-"].update(filename)
            window["-IMAGE-"].update(filename=filename)

        except Exception:
            pass

window.close()
