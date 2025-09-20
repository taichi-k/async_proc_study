実行手順
```sh
cd redis-streams
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# ターミナルA
python worker.py
# ターミナルB
python producer.py
```

失敗テスト
`producer.py` の `subject` を `"Hello FAIL"` にして再投入。

保留メッセージ確認
`docker exec -it $(docker ps --filter ancestor=redis:7 -q) redis-cli XPENDING jobs workers`