import json
import re

from sqlalchemy import select
from db import DataEmbedding, PatternEmbedding
from generated.recommendation_pb2_grpc import RecommendationHttpServicer
from google.protobuf import struct_pb2

from embedding import embeddingData, embeddingPattern
from helper import DatabaseInstance, DeviceInstance, ModelInstance, embeddingItem, getStrFromStruct, hotItem
from hot import hotImpl
import generated.recommendation_pb2 as recommendation__pb2


class RecommendationHttpServicerImpl(RecommendationHttpServicer):
    def Embedding(self, request, context):
        response = recommendation__pb2.EmbeddingResponse()
        typeItems = []
        items = []
        pattern = r'@(.*?)@'
        for p in request.pattern:
            matches = re.findall(pattern, p)
            value = p
            for d in request.data:
                if d.Is(struct_pb2.Struct.DESCRIPTOR):
                    struct_data = struct_pb2.Struct()
                    d.Unpack(struct_data)
                    for m in matches:
                        v = getStrFromStruct(struct_data, m)
                        if v:
                            value = re.sub("@"+m+"@", v, value)
                    items.append(embeddingItem(getStrFromStruct(struct_data, "id"), value, p))
            typeItems.append(embeddingItem(p, p))
        embeddingPattern(ModelInstance(), typeItems)
        embeddingData(ModelInstance(), items)
        for item in items:
            res = recommendation__pb2.EmbeddingResponse.EmbeddingResult()
            res.id = item.id
            res.pattern = item.pattern
            res.embedding.extend(item.embedding)
            response.res.append(res)
        return response

    def Hot(self, request, context):
        inputData = []
        model = ModelInstance()
        d = DeviceInstance().get()
        # TODO: read data from database
        session = DatabaseInstance().session()
        stmt = select(PatternEmbedding)
        typeText = [hotItem(item.id, item.embedding) for item in session.scalars(stmt)]
        typeIds = hotImpl(model, d, request.pattern, typeText, 1)
        tt = typeIds[0]
        stmt = select(DataEmbedding).where(DataEmbedding.pattern == tt)
        response = recommendation__pb2.HotResponse()
        for word in request.word:
            for item in session.scalars(stmt):
                inputData .append(hotItem(item.id, item.embedding))
            ids = hotImpl(model, d, word, inputData, request.k)
            res = recommendation__pb2.HotResponse.HotResult()
            res.count = len(ids)
            res.word = word
            res.ids.extend(ids)
            response.res.append(res)
        return response
