from gevent import monkey

monkey.patch_all()
from server import app
