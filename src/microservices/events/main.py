import asyncio
import contextlib
import datetime
import json
import logging
import os
from typing import Any, Dict, Literal, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import Body, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from schemas.events import Event, EventResponse, MovieEvent, PaymentEvent, UserEvent

app = FastAPI()

ROOT_ENDPOINT = "/api/events"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [events-service] %(message)s",
)

producer: Optional[AIOKafkaProducer] = None
consumer: Optional[AIOKafkaConsumer] = None
consumer_task: Optional[asyncio.Task] = None


def _topic_for(kind: Literal["movie", "user", "payment"]) -> str:
    return {"movie": "movie-events", "user": "user-events", "payment": "payment-events"}[
        kind
    ]


def _now_iso() -> str:
    # you imported the module, so use datetime.datetime.now
    return (
        datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )


async def _produce_and_response(kind: str, payload: Dict[str, Any]) -> JSONResponse:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    event: Event = Event(
        id=os.urandom(8).hex(),
        type=kind,
        timestamp=_now_iso(),
        payload=payload,
    )
    topic = _topic_for(kind)  # type: ignore[arg-type]
    value = json.dumps(
        event.dict() if hasattr(event, "dict") else event.model_dump()
    ).encode("utf-8")
    key = kind.encode("utf-8")

    meta = await producer.send_and_wait(topic, value=value, key=key)

    body = (
        EventResponse(
            status="success",
            partition=meta.partition,
            offset=meta.offset,
            event=event,
        ).dict()
        if hasattr(EventResponse, "dict")
        else EventResponse(
            status="success", partition=meta.partition, offset=meta.offset, event=event
        ).model_dump()
    )

    return JSONResponse(status_code=status.HTTP_201_CREATED, content=body)


async def _consume_loop() -> None:
    assert consumer is not None
    try:
        async for msg in consumer:
            try:
                logging.info(
                    "Consumed topic=%s partition=%s offset=%s value=%s",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    msg.value.decode("utf-8"),
                )
            except Exception as e:
                logging.exception("Failed to process message: %s", e)
    except asyncio.CancelledError:
        logging.info("Consumer loop cancelled.")
    except Exception as e:
        logging.exception("Consumer loop error: %s", e)


@app.on_event("startup")
async def on_startup() -> None:
    global producer, consumer, consumer_task
    logging.info("Starting Kafka producer/consumer (brokers: %s)", KAFKA_BROKERS)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    await producer.start()

    consumer = AIOKafkaConsumer(
        "user-events",
        "payment-events",
        "movie-events",
        bootstrap_servers=KAFKA_BROKERS,
        group_id="events-service",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    consumer_task = asyncio.create_task(_consume_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global producer, consumer, consumer_task
    logging.info("Shutting down...")
    if consumer_task:
        consumer_task.cancel()
        with contextlib.suppress(Exception):
            await consumer_task
    if consumer:
        await consumer.stop()
    if producer:
        await producer.stop()


# health
@app.get(f"{ROOT_ENDPOINT}/health")
def health_check():
    return {"status": True}


# endpoints: make them async and await the coroutine
@app.post(f"{ROOT_ENDPOINT}/movie", status_code=201, response_model=EventResponse)
async def create_movie_event(event: MovieEvent = Body(...)):
    return await _produce_and_response("movie", event.dict())


@app.post(f"{ROOT_ENDPOINT}/user", status_code=201, response_model=EventResponse)
async def create_user_event(event: UserEvent = Body(...)):
    return await _produce_and_response("user", event.dict())


@app.post(f"{ROOT_ENDPOINT}/payment", status_code=201, response_model=EventResponse)
async def create_payment_event(event: PaymentEvent = Body(...)):
    return await _produce_and_response("payment", event.dict())
