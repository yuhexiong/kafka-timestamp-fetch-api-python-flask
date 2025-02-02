# Kafka Timestamp Fetch API

提供一個 API，根據指定的時間範圍從 Kafka 獲取訊息。

## Overview

- 語言：Python v3.12  
- Web 框架：Flask v2.2.5  
- Kafka 工具：kafka-python-ng v2.0.3  

## Run

```bash
python app.py
```
Flask 服務將會運行於 `localhost:5000`。  

## API
- POST /kafka/fetch

範例請求：
```json
{
    "bootstrap_servers": "localhost:9092",
    "topic": "my_topic",
    "start_time": "2025-01-18 00:00:00",
    "end_time": "2025-01-20 00:00:00"
}
```

範例回應：
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