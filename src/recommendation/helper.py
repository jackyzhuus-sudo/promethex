
from functools import cached_property
from threading import Lock
from google.protobuf import struct_pb2
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine
from torch import device
import torch
from sqlalchemy.orm import Session
import os


class embeddingItem:
    def __init__(self, id: str, text: str, pattern: str = "", embedding: list = []):
        self.id = id
        self.text = text
        self.pattern = pattern
        self.embedding = embedding


class hotItem:
    def __init__(self, id: str, embedding: list = []):
        self.id = id
        self.embedding = embedding


def getStrFromStruct(struct_data: struct_pb2.Struct, field: str):
    v = struct_data.fields[field]
    if v:
        if v.string_value:
            return v.string_value
        elif v.number_value:
            return str(v.number_value)
    return ""


def singleton(cls):
    instances = {}
    lock = Lock()

    def wrapper(*args, **kwargs):
        with lock:  # 加锁确保线程安全
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
            return instances[cls]
    return wrapper


@singleton
class ModelInstance(SentenceTransformer):
    def __init__(self):
        print("loading model")
        super().__init__('all-MiniLM-L12-v2', device=DeviceInstance().get())
        print("model loaded")


@singleton
class DeviceInstance:
    def __init__(self):
        if torch.backends.mps.is_available():
            self.device = device("mps")
        else:
            self.device = device("cpu")

    def get(self):
        return self.device


@singleton
class DatabaseInstance:
    def __init__(self):
        self.engine = self._create_engine_from_env()

    def get(self):
        return self.engine

    def session(self):
        return Session(self.engine)

    def configure(self, engine):
        self.engine = engine

    def _create_engine_from_env(self):
        url = os.getenv("DATABASE_URL")
        if not url:
            driver = os.getenv("DB_DRIVER", "psycopg2")
            user = os.getenv("DB_USER", "myuser")
            password = os.getenv("DB_PASSWORD", "mypassword")
            host = os.getenv("DB_HOST", "db")
            port = os.getenv("DB_PORT", "5432")
            name = os.getenv("DB_NAME", "embedding")
            url = f"postgresql+{driver}://{user}:{password}@{host}:{port}/{name}"
        echo_env = os.getenv("DB_ECHO", "false").lower()
        echo = echo_env in ("1", "true", "yes", "on")
        return create_engine(url, echo=echo)
