# Streaming Needs a Lot of Pieces to Work

#### HTTP Interceptor to intercept request
- API event middleware - function createAPIEventMiddleware(res, res, next)

#### Kafka Producer to produce data as log
- function name - sendMessageToKafka
- provide topic and message

#### Big Computing Architecture for Streaming Pipeline
- Lambda
- Kappa

#### Lambda Architecture
- Optimize for latency and correctness
- Double pipeline/codebase, 1 batch and 1 streaming
    - Batch for backup if streaming fail
- Double code so a little pain in terms of complexity