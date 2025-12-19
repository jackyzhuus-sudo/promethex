#!/usr/bin/env python3

import json
import torch
import inspect
from sentence_transformers import SentenceTransformer
import pandas as pd

from db import DataEmbedding, PatternEmbedding
from helper import DatabaseInstance, embeddingItem


def embeddingPattern(model: SentenceTransformer, items: list[embeddingItem]):
    res = embedding(model, items)
    with DatabaseInstance().session() as conn:
        for item in items:
            try:
                conn.add(PatternEmbedding(id=item.id, embedding=item.embedding))
                conn.commit()
            except:
                conn.rollback()
    return len(res)


def embeddingData(model: SentenceTransformer, items: list[embeddingItem]):
    embedding(model, items)
    with DatabaseInstance().session() as conn:
        for item in items:
            try:
                conn.add(DataEmbedding(id=item.id, embedding=item.embedding, pattern=item.pattern))
                conn.commit()
            except:
                conn.rollback()


def embedding(model: SentenceTransformer, items: list[embeddingItem]):
    texts = [item.text for item in items]
    # 3.1) Compile the model for faster inference (requires PyTorch >= 2.0)
    try:
        model = torch.compile(model, mode="reduce-overhead")
        print("Model compiled with torch.compile (reduce-overhead).")
    except Exception as e:
        print("torch.compile not available or failed:", e)

    # 4) Inspect and print the default batch_size from the encode signature
    sig = inspect.signature(model.encode)
    default_bs = sig.parameters['batch_size'].default
    print(f"Default batch size (from signature): {default_bs}")

    # 5) (Optional) override batch size or just use the default
    batch_size = default_bs
    print(f"Encoding with batch size = {batch_size}")

    # 6) Generate embeddings
    print('Generating embeddings...')
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=False
    )

    # 7) Append embeddings to original data
    for item, emb in zip(items, embeddings):
        del item.text
        item.embedding = emb.tolist()
    return items
