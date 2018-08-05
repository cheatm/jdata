import os

MONGODB_URI = os.environ.get("MONGODB_URI", "localhost")
ADDR = os.environ.get("ADDR", "tcp://data.quantos.org:8910")
USERNAME = os.environ.get("USERNAME", "USERNAME")
PASSWORD = os.environ.get("PASSWORD", "PASSWORD")

def get_env(name):
    return os.environ.get(name, name)
