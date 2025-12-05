import time,json, argparse,os, requests
from kafka import KafkaProducer

API_URL = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={SYMBOL}&apikey={apikey}"


def get_quote(symbol, apikey):
    url = API_URL.format(SYMBOL=symbol, apikey=apikey)
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apikey", required=False, default=os.getenv('ALPHAVANTAGE_API_KEY'), help='Alpha Vantage API Key')
    parser.add_argument("--symbol", default==os.getenv("SAMPLE_SYMBOL","IBM"), help="stock symbol")
    parser.add_argument("--interval", type=int, default=60, help="seconds between polls")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER","localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC","stocks_raw"))
    args=parser.parse_args()

    if not args.apikey:
        raise SystemExit("AIphaVantage API key required (pass --apikey or set ALPHAVANTAGE_API_KEY)")
    
    producer=KafkaProducer(
        bootstrap_servers = [args.broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms = 10
    )



    print(f"Producer: sending to {args.topic} via {args.broker} for symbol {args.symbol}")
    while True:
        try:
            data = get_quote(args.symbol, args.apikey)
            payload= {"sysbol":args.symbol, "timestamp": int(time.time()),"raw": data}
            producer.send(args.topic, payload)
            producer.flush()
            print("sent", payload["timestamp"])
        except Exception as e:
            print("Error:", e)
        time.sleep(args.interval)

if __name__ == "__main__":
    main()