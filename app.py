from flask import Flask, request, jsonify
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timezone

app = Flask(__name__)

@app.route('/kafka/fetch', methods=['POST'])
def fetch_kafka():
    # 從請求體中獲取 JSON 數據
    data = request.get_json()

    # 驗證必要參數是否都有提供
    required_fields = ['bootstrap_servers', 'topic', 'start_time', 'end_time']
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"缺少必需的參數: {field}"}), 400

    bootstrap_servers = data['bootstrap_servers']
    topic = data['topic']
    start_time_str = data['start_time']
    end_time_str = data['end_time']

    # 轉換時間字串為 UTC 時間戳（毫秒）
    try:
        start_date = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        return jsonify({"error": "時間格式無效。請使用 'YYYY-MM-DD HH:MM:SS' 格式。"}), 400

    start_timestamp = int(start_date.timestamp() * 1000)  # 開始時間轉為毫秒
    end_timestamp = int(end_date.timestamp() * 1000)      # 結束時間轉為毫秒

    # 初始化 KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="group-id",
        auto_offset_reset="earliest",  # 如果找不到 offset，從最早的消息開始
        enable_auto_commit=False,      # 禁止自動提交 offset
        value_deserializer=lambda x: x.decode("utf-8")  # 解碼消息
    )

    # 獲取 Kafka topic 的所有 partition
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return jsonify({"error": f"Topic {topic} 不存在或沒有可用的 partition。"}), 404

    # 查找每個 partition 對應的起始 offset
    timestamps = {TopicPartition(topic, p): start_timestamp for p in partitions}
    start_offsets = consumer.offsets_for_times(timestamps)

    # 查找每個 partition 對應結束時間的 offset
    end_timestamps = {TopicPartition(topic, p): end_timestamp for p in partitions}
    end_offsets = consumer.offsets_for_times(end_timestamps)

    results = []

    for tp in partitions:
        start_offset_info = start_offsets.get(TopicPartition(topic, tp))
        start_offset = start_offset_info.offset if start_offset_info else None
        end_offset_info = end_offsets.get(TopicPartition(topic, tp))
        end_offset = end_offset_info.offset if end_offset_info else None

        if start_offset is None or end_offset is None:
            continue

        consumer.assign([TopicPartition(topic, tp)])  # 手動分配 partition
        consumer.seek(TopicPartition(topic, tp), start_offset)  # 設置起始 offset

        # 儲存該 partition 消費的消息
        partition_messages = []

        while True:
            message = next(consumer)
            message_timestamp = datetime.fromtimestamp(message.timestamp / 1000, tz=timezone.utc)
            readable_timestamp = message_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            partition_messages.append({
                "offset": message.offset,
                "value": message.value,
                "timestamp": readable_timestamp
            })

            # 停止條件：達成結束 offset 或結束時間
            if message.offset >= (end_offset - 1) or message.timestamp > end_timestamp:
                break

        if partition_messages:
            results.append({
                "partition": tp,
                "messages": partition_messages
            })

    consumer.close()

    if not results:
        return jsonify({"message": "在指定的時間範圍內沒有找到任何消息。"}), 404

    return jsonify({"data": results})

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
