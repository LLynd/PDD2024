import argparse
import json
import random
import threading
import time

from kafka import KafkaProducer

STOP_EVENT = threading.Event()

def stream_1_generator():
    index = 0
    while True:
        index += 1
        if random.uniform(0, 1) < 0.05:
            continue
        yield (index, random.uniform(0, 1))


def stream_2_generator():
    break_point = 300_000
    index = 0
    while index < break_point:
        index += 1
        if random.uniform(0, 1) < 0.1:
            continue
        yield (index, random.gauss(2, 1))
    while index < break_point * 2:
        index += 1
        if random.uniform(0, 1) < (index - break_point)/break_point:
            yield (index, random.uniform(0, 1))
        else:
            yield (index, random.gauss(2, 1))
            
    while True:
        index += 1
        if random.uniform(0, 1) < 0.01:
            continue
        yield (index, random.uniform(0, 1))



def producer(topic: str, stream_generator, server: str):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    gen = stream_generator()
    while not STOP_EVENT.is_set():
        time_point, value = next(gen)
        # print(f"Sending {time_point} {value} to {topic}")
        producer.send(topic, {"time_point": time_point, "value": value})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic1", type=str, default="topic1")
    parser.add_argument("--topic2", type=str, default="topic2")
    parser.add_argument("--bootstrap_server", type=str, default="localhost:9092")
    args = parser.parse_args()

    producer1 = threading.Thread(target=producer, args=(args.topic1, stream_1_generator, args.bootstrap_server))
    producer2 = threading.Thread(target=producer, args=(args.topic2, stream_2_generator, args.bootstrap_server))

    producer1.start()
    producer2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        STOP_EVENT.set()
        producer1.join()
        producer2.join()

if __name__ == '__main__':
    main()