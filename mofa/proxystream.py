from typing import Collection, Optional, Union, Dict, Any, Tuple
import logging
import uuid

from colmena.exceptions import KillSignalException, TimeoutException
from colmena.models import SerializationMethod, Result, ResourceRequirements
from colmena.queue.base import ColmenaQueues, QueueRole

import redis

from proxystore.connectors.redis import RedisConnector
from proxystore.store import get_store, register_store
from proxystore.store.base import Store
from proxystore.stream.shims.redis import RedisQueuePublisher, RedisQueueSubscriber
from proxystore.stream.interface import StreamConsumer, StreamProducer

logger = logging.getLogger(__name__)


class ProxyStreamRedisQueues(ColmenaQueues):
    def __init__(
        self,
        topics: Collection[str],
        hostname: str = "127.0.0.1",
        port: int = 6379,
        prefix: str = uuid.uuid4(),
        serialization_method: Union[
            str, SerializationMethod
        ] = SerializationMethod.PICKLE,
        keep_inputs: bool = True,
    ) -> None:
        super().__init__(
            topics,
            serialization_method,
            keep_inputs=keep_inputs,
            proxystore_name=None,
            proxystore_threshold=None,
        )
        self.hostname = hostname
        self.port = port
        self.prefix = prefix
        self.reconnect = False
        self.connect()

    def __setstate__(self, state):
        self.__dict__ = state

        if self.reconnect:
            self.connect()

    def __getstate__(self):
        state = super().__getstate__()

        state.pop("store")
        state.pop("producer")
        state.pop("consumers")

        state["reconnect"] = True
        return state

    def connect(self):
        logger.info("Creating ProxyStream interfaces")
        store_name = "proxystream-redis-store"
        store = get_store(store_name)
        if store is None:
            connector = RedisConnector(self.hostname, self.port)
            self.store = Store("proxystream-redis-store", connector)
            register_store(self.store)
        else:
            self.store = store

        publisher = RedisQueuePublisher(self.hostname, self.port)
        self.producer = StreamProducer(publisher, {None: self.store})

        self.consumers = {}

        topic = f"{self.prefix}-requests"
        subscriber = RedisQueueSubscriber(self.hostname, self.port, topic)
        self.consumers[topic] = StreamConsumer(subscriber)

        for topic in self.topics:
            topic = f"{self.prefix}-{topic}-results"
            subscriber = RedisQueueSubscriber(
                self.hostname, self.port, topic, timeout=1
            )
            consumer = StreamConsumer(subscriber)
            self.consumers[topic] = consumer

    def disconnect(self):
        self.producer.close()
        for consumer in self.consumers.values():
            consumer.close()
        self.store.close()

    def get_result(
        self,
        topic: str = "default",
        timeout: Optional[float] = None,
    ) -> Optional[Result]:
        self._check_role(QueueRole.CLIENT, "get_result")

        # Get a value
        queue = f"{self.prefix}-{topic}-results"
        consumer = self.consumers[queue]
        try:
            result_json, value = consumer.next_with_metadata()
        except TimeoutError as e:
            raise TimeoutException() from e

        # Parse the value and mark it as complete
        result_obj = Result.parse_obj(result_json)
        result_obj.time.deserialize_results = result_obj.deserialize()
        result_obj.value = value
        result_obj.mark_result_received()

        # Some logging
        logger.info(f"Client received a {result_obj.method} result with topic {topic}")

        # Update the list of active tasks
        with self._active_lock:
            self._active_tasks.discard(result_obj.task_id)
            if len(self._active_tasks) == 0:
                self._all_complete.set()

        return result_obj

    def send_inputs(
        self,
        *input_args: Any,
        method: str = None,
        input_kwargs: Optional[Dict[str, Any]] = None,
        keep_inputs: Optional[bool] = None,
        resources: Optional[Union[ResourceRequirements, dict]] = None,
        topic: str = "default",
        task_info: Optional[Dict[str, Any]] = None,
    ) -> str:
        self._check_role(QueueRole.CLIENT, "send_inputs")

        # Make sure the queue topic exists
        if topic not in self.topics:
            raise ValueError(
                f'Unknown topic: {topic}. Known are: {", ".join(self.topics)}'
            )

        # Make fake kwargs, if needed
        if input_kwargs is None:
            input_kwargs = dict()

        # Determine whether to override the default "keep_inputs"
        _keep_inputs = self.keep_inputs
        if keep_inputs is not None:
            _keep_inputs = keep_inputs

        # Create a new Result object
        result = Result(
            (input_args, input_kwargs),
            method=method,
            keep_inputs=_keep_inputs,
            serialization_method=self.serialization_method,
            task_info=task_info,
            resources=resources or ResourceRequirements(),
            topic=topic,
        )

        inputs = result.inputs
        result.inputs = ((), {})
        result.time.serialize_inputs, proxies = result.serialize()

        self.producer.send(
            f"{self.prefix}-requests",
            inputs,
            evict=False,
            metadata=result.dict(),
        )
        logger.info(f"Client sent a {method} task with topic {topic}.")

        # Store the task ID in the active list
        with self._active_lock:
            self._active_tasks.add(result.task_id)
            self._all_complete.clear()
        return result.task_id

    def get_task(self, timeout: float = None) -> Tuple[str, Result]:
        self._check_role(QueueRole.SERVER, "get_task")

        # Pull a record off of the queue
        queue = f"{self.prefix}-requests"
        consumer = self.consumers[queue]
        try:
            task_json, inputs = consumer.next_with_metadata()
        except StopIteration as e:
            raise KillSignalException() from e

        # Get the message
        task = Result.parse_obj(task_json)
        task.inputs = inputs
        task.mark_input_received()

        topic = task.topic
        logger.debug(f"Received a task message with topic {topic} inbound queue")

        return topic, task

    def send_kill_signal(self):
        self._check_role(QueueRole.CLIENT, "send_kill_signal")
        self.producer.close_topics(f"{self.prefix}-requests")

    def send_result(self, result: Result):
        self._check_role(QueueRole.SERVER, "send_result")
        result.mark_result_sent()

        # TODO: this is innefficient
        result.deserialize()

        value = result.value
        result.value = None

        queue = f"{self.prefix}-{result.topic}-results"
        self.producer.send(queue, value, evict=True, metadata=result.dict())

    def _get_request(self, timeout: int = None) -> Tuple[str, str]:
        raise NotImplementedError

    def _send_request(self, message: str, topic: str):
        raise NotImplementedError

    def _get_result(self, topic: str, timeout: int = None) -> str:
        raise NotImplementedError

    def _send_result(self, message: str, topic: str):
        raise NotImplementedError

    def flush(self):
        all_queues = [f"{self.prefix}-requests"]
        for topic in self.topics:
            all_queues.append(f"{self.prefix}-{topic}-results")
        try:
            self.producer.publisher._redis_client.delete(*all_queues)
        except redis.exceptions.ConnectionError:
            logger.warning(
                f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}"
            )
            raise
