# http-hammer and vLLM Architecture

## 1. Main Idea

`http-hammer` is outside the model server.

It does not run inference. It simulates clients, sends controlled HTTP traffic, and measures how the serving path behaves.

```text
Parquet dataset
    |
    v
http-hammer
    |
    v
vLLM APIServer
    |
    v
Core Engine
```

## 2. Inside http-hammer

```text
config.json / config-hstu.json
    |
    v
Dataset loader
    |
    | reads Parquet rows
    | maps columns using body_fields
    | merges template fields
    v
Vector of JSON request bodies
    |
    v
Injector thread
    |
    | controls RPS
    | sends request indexes to workers
    | sends periodic ping requests
    v
Worker threads
    |
    | keep connection pools open
    | send HTTP/1.1 requests
    | parse Content-Length responses
    | update latency histograms
    v
UI + metrics
```

## 3. Two Request Modes

### Chat / OneRec

From `config.json`:

```text
route: /v1/chat/completions
model: OpenOneRec/OneRec-1.7B
body_fields:
  messages -> messages
template:
  use_beam_search: true
  n: 128
  max_tokens: 5
```

This mode sends OpenAI-style chat completion requests.

### HSTU / Recommend

From `config-hstu.json`:

```text
route: /recommend
model: movielens-1m
body_fields:
  request_id -> request_id
  prompt -> prompt
template:
  priority: 0
```

This mode sends recommender requests.

These are separate runs. The hammer does not mix the two API contracts unless the config is changed.

## 4. vLLM Serving Architecture

```text
HTTP client
  http-hammer
    |
    v
APIServer
  owns HTTP routes
  validates request format
  translates API protocol to engine requests
  formats final responses
    |
    v
Async engine client
  bridges serving code to engine lifecycle
    |
    v
Core Engine
  scheduler
  KV cache manager
  model executor
  decode/prefill execution
```

The APIServer owns the external API contract.

The Core Engine owns model execution and scheduling.

## 5. End-to-End Hammer Run

```text
1. Load Parquet rows
2. Build JSON bodies
3. Set target RPS
4. Injector enqueues work
5. Workers send HTTP requests
6. APIServer receives traffic
7. Core Engine runs model work
8. Workers parse responses
9. UI shows latency, pings, sent rate, receive rate, and errors
```

## 6. What http-hammer Helps Explain

- Can the APIServer keep accepting requests at the target rate?
- Does the Core Engine fall behind when beam search or recommender traffic increases?
- Are pings and metrics healthy while real requests are in flight?
- Which signal changes first: latency, errors, free connections, or server metrics?

## 7. Demo Commands

### Chat route

```bash
cd /data/users/sagiv/git/http-hammer
cargo run -- \
  -c config.json \
  -f data/your_chat_dataset.parquet \
  -r 1
```

### Recommend route

```bash
cd /data/users/sagiv/git/http-hammer
cargo run -- \
  -c config-hstu.json \
  -f data/recommend_random.parquet \
  -r 1
```

### SSH tunnel, if needed

```bash
ssh -L 8000:10.10.12.102:8000 user@pick
```

Then verify:

```bash
curl -i http://127.0.0.1:8000/ping
```
