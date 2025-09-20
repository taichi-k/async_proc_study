起動方法
```sh
cd rabbitmq
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python worker.py
python producer.py # 別ターミナルで
```