import os


def make_folder():
    try:
        os.mkdir(f"{os.path.expanduser('~')}/dag_data")
    except FileExistsError:
        pass


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)
