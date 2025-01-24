# Kafka Timestamp Fetch API

Provides an API to fetch messages from Kafka based on a specified timestamp range.

## Overview

- Language: Python v3.12
- Web Framework: Flask v2.2.5
- Kafka Tool: kafka-python-ng v2.0.3

## Run

```bash
python app.py
```
The Flask service will be available at `localhost:5000`.  


## API
- POST /kafka/fetch

Example request:
```json
{
    "bootstrap_servers": "localhost:9092",
    "topic": "my_topic",
    "start_time": "2025-01-18 00:00:00",
    "end_time": "2025-01-20 00:00:00"
}
```

Example response:
```json
{
    "data": [
        {
            "partition": 0,
            "messages": [
                {
                    "offset": 100,
                    "value": "Message A",
                    "timestamp": "2025-01-18 00:00:00"
                },
                {
                    "offset": 101,
                    "value": "Message B",
                    "timestamp": "2025-01-18 00:01:00"
                }
            ]
        }
    ]
}
```
