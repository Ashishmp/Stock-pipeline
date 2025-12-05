# Stock Pipeline

Real-Time stock market analysis pipeline(local starter)

Steps:
1. Start kafka: 'cd kafka' -> 'docker compose up -d'
2. create topic: run kafka-topic inside container (Kindly refer docs)
3. Activate venv and install dependencies: 'python -m venv venv; .\.venv\Scripts\Activate.ps1; pip install -r requirement.txt'
4. Run consumer: 'python .\Scripts\consume_test.py'
5. Run local processor: 'python .\spark\local_stream_processor.py data\sample_stock.json'
6. Run streaming processor: 'python .\spark\stream_processor.py data\output'
7. Run producer: 'python .\Scripts\fetch_stocks.py --apikey <apikey> --symbol AAPL --interval 60'



Refer docs /how_to_bigquery.md for BigQuery steps.
