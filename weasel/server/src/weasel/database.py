from contextlib import contextmanager

import sqlmodel
from sqlmodel import Session, create_engine

from .models import *  # noqa: F403 # this is needed for database initialization

DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(DATABASE_URL, echo=True)


def init_db():
    # Import models here to create tables
    sqlmodel.SQLModel.metadata.create_all(engine)


@contextmanager
def get_session():
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
