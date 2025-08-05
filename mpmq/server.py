import asyncio, logging
from grpc import aio
from prometheus_client import start_http_server
from . import broker, queue_pb2, queue_pb2_grpc, metrics

class QueueService(queue_pb2_grpc.QueueServiceServicer):
    def __init__(self, brk: broker.Broker):
        self._brk = brk

    async def Produce(self, request, context):
        off = await self._brk.publish(request.topic, request.value)
        metrics.PUBLISHED.inc()
        return queue_pb2.ProduceReply(offset=off)

    async def Consume(self, request, context):
        recs = await self._brk.consume_group(request.topic, request.group, request.max_records)
        metrics.CONSUMED.inc(len(recs))
        return queue_pb2.ConsumeReply(records=recs)

async def serve() -> None:
    server = aio.server(options=[("grpc.so_reuseport", 0)])
    queue_pb2_grpc.add_QueueServiceServicer_to_server(QueueService(broker.Broker()), server)
    server.add_insecure_port("[::]:50051")
    await server.start()
    logging.info("Broker listening on :50051")
    start_http_server(8000)  # Prometheus
    await server.wait_for_termination()

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(serve())