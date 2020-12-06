from os.path import dirname
from sys import path as sys_path

root_dir = dirname(dirname(dirname(__file__)))
sys_path.append(root_dir)
