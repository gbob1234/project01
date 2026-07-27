import json
from typing import Any

import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition


def read_recent_messages(
    consumer: Consumer,
    topic: str,
    partition: int,
    recent_count: int = 100,
    batch_size: int = 100,
    timeout: float = 3.0,
) -> list[dict[str, Any]]:
    topic_partition = TopicPartition(topic, partition)

    low, high = consumer.get_watermark_offsets(
        topic_partition,
        timeout=10.0,
        cached=False,
    )

    if high <= low:
        return []

    start_offset = max(low, high - recent_count)

    # 시작 오프셋을 assign 단계에서 직접 지정
    consumer.assign([
        TopicPartition(topic, partition, start_offset)
    ])

    records: list[dict[str, Any]] = []

    while True:
        remaining = high - start_offset

        if remaining <= 0:
            break

        messages = consumer.consume(
            num_messages=min(batch_size, remaining),
            timeout=timeout,
        )

        if not messages:
            # 조회 시점의 high까지 아직 모두 가져오지 못했을 수 있으므로
            # 운영 환경에서는 재시도 횟수를 별도로 두는 것이 안전함
            break

        reached_end = False

        for msg in messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    reached_end = True
                    continue

                raise RuntimeError(
                    f"Kafka consume error: "
                    f"topic={topic}, partition={partition}, "
                    f"error={msg.error()}"
                )

            # 최초 watermark 조회 당시 범위를 넘어선 신규 메시지는 제외
            if msg.offset() >= high:
                reached_end = True
                break

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                records.append({
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "message_id": None,
                    "parse_error": str(exc),
                })
                continue

            records.append({
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "message_id": value.get("message_id"),
                "value": value,
                "parse_error": None,
            })

            start_offset = msg.offset() + 1

        if reached_end or start_offset >= high:
            break

    consumer.unassign()
    return records


def read_topic_recent_messages(
    consumer: Consumer,
    topic: str,
    recent_count_per_partition: int = 100,
) -> pd.DataFrame:
    metadata = consumer.list_topics(topic=topic, timeout=10.0)

    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None:
        raise RuntimeError(f"Topic not found: {topic}")

    if topic_metadata.error is not None:
        raise RuntimeError(
            f"Failed to get topic metadata: {topic_metadata.error}"
        )

    records: list[dict[str, Any]] = []

    for partition_id in sorted(topic_metadata.partitions):
        partition_records = read_recent_messages(
            consumer=consumer,
            topic=topic,
            partition=partition_id,
            recent_count=recent_count_per_partition,
        )
        records.extend(partition_records)

    return pd.DataFrame(records)

