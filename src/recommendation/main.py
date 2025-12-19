
from concurrent import futures
import os
import grpc
from sqlalchemy import text, create_engine
from db import Base
import grpcImpl

from generated.recommendation_pb2_grpc import add_RecommendationHttpServicer_to_server
from helper import DatabaseInstance


def main():
    def _get_env(name, default):
        v = os.getenv(name)
        return v if v is not None and v != "" else default
    with DatabaseInstance().session() as session:
        session.execute(text('CREATE EXTENSION IF NOT EXISTS vector'))
        session.commit()
        Base.metadata.create_all(DatabaseInstance().get())
        session.commit()
    workers = int(_get_env("GRPC_WORKERS", "10"))
    port = int(_get_env("GRPC_PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    add_RecommendationHttpServicer_to_server(grpcImpl.RecommendationHttpServicerImpl(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"gprc server started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    main()
