from inspect import getsourcefile
import ctypes
import os


windll = ctypes.windll if os.name == 'nt' else None # for Mac users


class WindowName:
    def __init__(self, accs_amount: int):
        try: self.path = os.path.abspath(getsourcefile(lambda: 0)).split("\\")[-3]
        except: self.path = os.path.abspath(getsourcefile(lambda: 0)).split("/")[-3]

        self.accs_amount = accs_amount
        self.accs_done = 0
        self.modules_amount = 0
        self.modules_done = 0

        self.update_name()

    def update_name(self):
        if os.name == 'nt':
            windll.kernel32.SetConsoleTitleW(f'Lighter [{self.accs_done}/{self.accs_amount}] | {self.path}')

    def add_acc(self):
        self.accs_done += 1
        self.update_name()

    def add_module(self, modules_done=1):
        self.modules_done += modules_done
        self.update_name()

    def new_acc(self):
        self.accs_done += 1
        self.modules_amount = 0
        self.modules_done = 0
        self.update_name()

    def set_modules(self, modules_amount: int):
        self.modules_done = 0
        self.modules_amount = modules_amount
        self.update_name()
