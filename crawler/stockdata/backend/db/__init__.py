from .router import Router
from .db import *

router = Router()


def get_db_router():
    return router
