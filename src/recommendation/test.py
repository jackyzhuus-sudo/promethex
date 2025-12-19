
import json

import grpc

from generated.recommendation_pb2 import EmbeddingRequest, HotRequest
from google.protobuf.any_pb2 import Any
from google.protobuf import struct_pb2

from generated.recommendation_pb2_grpc import RecommendationHttpStub


def test_embedding(stub: RecommendationHttpStub):
    request_data = EmbeddingRequest()
    data = []
    with open('output_100x.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            any_obj = Any()
            struct_obj = struct_pb2.Struct()
            struct_obj.update(item)  # 将字典内容注入 Struct
            any_obj.Pack(struct_obj)
            request_data.data.append(any_obj)
        request_data.pattern.append("@title@ : @description@")
        request_data.pattern.append("@title@ : @volume@")
        # req_data = request_data.SerializeToString()# 序列化
        res = stub.Embedding(request_data)
        print(res)


def test_hot(stub: RecommendationHttpStub):
    request_data = HotRequest()
    request_data.pattern = "title"
    request_data.k = 10
    request_data.word.append("ethereum")
    request_data.word.append("bitcoin")
    res = stub.Hot(request_data)
    print(res)


if __name__ == '__main__':
    # test_my_protobuf()
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = RecommendationHttpStub(channel)
        test_embedding(stub)
        test_hot(stub)
