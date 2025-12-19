#!/usr/bin/env python3

import torch
from sentence_transformers import SentenceTransformer
import torch.nn.functional as F

from helper import hotItem


def hotImpl(model: SentenceTransformer, device: torch.device, text_to_embed: str, data: list[hotItem], k: int):
    embeddings = torch.tensor([item.embedding for item in data], device=device)

    print(f'Embedding query: "{text_to_embed}"')
    # encode returns a tensor when convert_to_tensor=True
    query_embedding = model.encode(
        [text_to_embed], convert_to_tensor=True)[0].to(device)

    # 5) Normalize embeddings for cosine similarity
    emb_norm = F.normalize(embeddings, p=2, dim=1)
    query_norm = F.normalize(query_embedding, p=2, dim=0)

    # 6) Compute cosine similarity scores
    similarities = emb_norm @ query_norm  # shape [num_items]

    # 7) Get top-K most similar items
    num_items = similarities.size(0)
    top_k = min(k, num_items)
    values, indices = torch.topk(similarities, k=top_k)
    res = []
    # 8) Print results
    print(f"\nTop {top_k} most similar items to '{text_to_embed}':")
    for rank, (idx, score) in enumerate(zip(indices.tolist(), values.tolist()), start=1):
        item = data[idx]
        res.append(item.id)
        print(f"{rank}. {item.id}  –  cosine = {score:.4f}")
    return res
