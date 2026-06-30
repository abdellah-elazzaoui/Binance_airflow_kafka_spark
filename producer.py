from confluent_kafka import Producer
from admin import Admin
from uuid import uuid4
import requests
import time
import threading
import json

def delivery_report(err,msg):
    if err is not None:
        print(f"Failed to delivery Message : {msg.key()}")
    print(f"the Message Successfully Deliver Key: {msg.key()} topic: {msg.topic()} Partition: {msg.partition()} offset: {msg.offset()}")    


class BinanceProducer:
    def __init__(self,bootstrapserver , topic , message_size = None , compression_type = None ,batch_size = None , waiting_time = None):
        self.bootstrapserver = bootstrapserver
        self.topic = topic
        config = {"bootstrap.servers":self.bootstrapserver}
        if message_size:
            config["message.max.bytes"] = message_size
        if compression_type :
            config["compression.type"] = compression_type
        if batch_size:
            config["batch.size"] = batch_size
        if waiting_time :
            config["linger.ms"]  = waiting_time
        config["partitioner"] = "random"    
        self.producer = Producer(config)
        self.symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT"]

    
    def get_price(self,symbol, prices_dict):
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                prices_dict[symbol] = response.json()['price']
            elif response.status_code == 202:
                time.sleep(1)
                # On repasse le dictionnaire pour maintenir la référence
                self.get_price(symbol, prices_dict)
            else:
                prices_dict[symbol] = None
        except Exception:
            prices_dict[symbol] = None
    def create_message(self,symbols):
        prices = {}
        threads = []
        
        for symbol in symbols:
            # On passe 'prices' dans les arguments du thread
            thread = threading.Thread(target=self.get_price, args=(symbol, prices))
            thread.start()
            threads.append(thread)
            
        for thread in threads:
            thread.join()
            
        current_time = time.strftime("%H:%M:%S")
        return current_time, prices    

    def send_message(self):
        try:
            current_time, prices = self.create_message(self.symbols)
            message = json.dumps(prices)
            if message:
                self.producer.produce(
                    topic = self.topic,
                    value=message,
                    key=str(current_time),
                    headers = {"correlation_id":str(uuid4())},
                    on_delivery = delivery_report
                )
                self.commit()
        except Exception as e:
            print(f"ERROR - {str(e)}")        

    def commit(self):
        self.producer.flush()        



if __name__ == "__main__":

    bootstrapserver = "localhost:9092,localhost:9093,localhost:9094"
    topic = "binance"
    a = Admin(bootstrapserver=bootstrapserver)
    a.create_topic(topic=topic , partition=2)
    producer = BinanceProducer(bootstrapserver,topic,4*1024,"snappy",batch_size=10000,waiting_time=100)
    try:
        producer.send_message()

    except KeyboardInterrupt :
        pass
    except Exception as e:
        print(f"ERROR - {str(e)}")
        producer.commit()
        print(f"ERROR - {str(e)}")