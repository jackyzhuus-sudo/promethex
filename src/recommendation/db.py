from typing import List
from typing import Optional
from pgvector.sqlalchemy import Vector
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class DataEmbedding(Base):
    __tablename__ = "data_embedding"

    id: Mapped[str] = mapped_column(primary_key=True)
    embedding: Mapped[Vector] = mapped_column(Vector(384))
    pattern: Mapped[str] = mapped_column(primary_key=True)


class PatternEmbedding(Base):
    __tablename__ = "pattern_embedding"

    id: Mapped[str] = mapped_column(primary_key=True)
    embedding: Mapped[Vector] = mapped_column(Vector(384))
