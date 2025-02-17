import random
import time
import json
from datetime import datetime
from google.cloud import pubsub_v1

class LogisticsEventGenerator:
    def __init__(self):
        self.products = [
            {"id": "prod_001", "name": "Product A"},
            {"id": "prod_002", "name": "Product B"},
            {"id": "prod_003", "name": "Product C"}
        ]
        
        self.warehouses = [
            {"id": "WH_TPE", "name": "Taipei"},
            {"id": "WH_TC", "name": "Taichung"},
            {"id": "WH_KH", "name": "Kaohsiung"}
        ]
        
        self.status_flow = [
            "ORDER_CREATED",
            "STOCK_CHECKING",
            "READY_TO_SHIP",
            "SHIPPING",
            "DELIVERED"
        ]
    
    def generate_event(self):
        """生成單一物流事件"""
        product = random.choice(self.products)
        warehouse = random.choice(self.warehouses)
        
        return {
            "event_id": f"evt_{int(time.time())}{random.randint(1000,9999)}",
            "order_id": f"ord_{int(time.time())}{random.randint(100,999)}",
            "product_id": product["id"],
            "status": random.choice(self.status_flow),
            "event_timestamp": datetime.now().isoformat(),
            "warehouse_id": warehouse["id"]
        }

def publish_messages(project_id, topic_name, generator, delay=False):
    """發布消息到 Pub/Sub"""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    print(f"開始發布消息到 {topic_path}")
    
    while True:
        try:
            # 生成事件
            event = generator.generate_event()
            
            # 轉換為 JSON 字串
            message_data = json.dumps(event).encode('utf-8')
            
            # 發布消息
            future = publisher.publish(topic_path, message_data)
            message_id = future.result()
            print(f"發布消息 {message_id}: {event['event_id']}")
            
            # 模擬延遲
            if delay:
                time.sleep(random.uniform(0, 2))
            else:
                time.sleep(0.5)
                
        except Exception as e:
            print(f"發生錯誤: {e}")
            continue

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate and publish logistics events')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--delay', action='store_true', help='Add random delay to events')
    
    args = parser.parse_args()
    
    generator = LogisticsEventGenerator()
    publish_messages(args.project_id, "logistics_events", generator, args.delay)
