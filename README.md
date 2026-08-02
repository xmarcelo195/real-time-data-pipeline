# Real-Time Bitcoin-Like Streaming Analytics

Plataforma modular para simular, ingerir e analisar transacoes UTXO-style em tempo real usando Kafka, Apache Flink, Redis, ClickHouse e Grafana.

## Arquitetura

O contrato principal do sistema e:

`TransactionSource -> RawTransactionEvent -> NormalizedTransactionEvent -> Kafka -> Flink`

O job Flink consome apenas `btc.transactions` no formato normalizado. Isso significa que a origem pode mudar de `SimulatedSource` para `BitcoinCoreSource` ou uma API externa sem refatorar o core stateful.

### Principios aplicados

- Ingestion layer plugavel via interface `TransactionSource`
- Normalizacao obrigatoria antes do Kafka
- Flink isolado da origem dos dados
- Estado UTXO particionado por endereco
- Event time com watermarks e tratamento de eventos atrasados
- Sinks operacionais e analiticos desacoplados do source

## Estrutura

- `ingestion-service/`
  - `app/sources/base.py`: contrato `TransactionSource`
  - `app/sources/simulated.py`: gerador UTXO realista com fan-out, consolidacao, whales, structuring e timestamps fora de ordem
  - `app/sources/bitcoin_core.py`: placeholder para RPC/ZMQ futuro
  - `app/normalizer.py`: conversao para evento canonico
- `flink-job/`
  - `app/job.py`: streaming engine com estado UTXO, saldos, AML-lite e sinks
- `clickhouse/init/01_schema.sql`
  - esquema analitico
- `grafana/`
  - provisionamento de datasource e dashboard
- `kafka-init/create-topics.sh`
  - criacao de topicos obrigatorios

## Evento normalizado

Schema minimo operacional:

```json
{
  "tx_id": "string",
  "timestamp": 1712966400000,
  "inputs": [
    {
      "address": "addr_00001",
      "value": 1.25,
      "prev_tx_id": "prev-tx",
      "prev_output_index": 0
    }
  ],
  "outputs": [
    {
      "address": "addr_00002",
      "value": 1.249,
      "output_index": 0
    }
  ],
  "source": "simulated",
  "ingest_time": 1712966401000
}
```

Os campos extras de UTXO (`prev_tx_id`, `prev_output_index`, `output_index`) foram mantidos dentro do evento normalizado porque sao necessarios para reconstruir o conjunto global de outputs nao gastos sem acoplamento ao source.

## O que o Flink faz

### 1. Event time

- Consome `btc.transactions`
- Atribui timestamps a partir do campo `timestamp`
- Gera watermarks com atraso tolerado configuravel
- Continua processando eventos atrasados e marca esses casos em metricas e alertas

### 2. UTXO state engine

- Extrai updates por endereco a partir de inputs e outputs normalizados
- Mantem `ListState` com UTXOs ativos por endereco
- Remove outputs gastos pelo par `(prev_tx_id, prev_output_index)`
- Adiciona novos outputs pelo par `(tx_id, output_index)`

### 3. Balance computation

- Atualiza saldo incrementalmente por endereco
- Persiste saldo corrente no Redis em `HSET balances <address> {...}`
- Persiste historico de atualizacoes no ClickHouse

### 4. AML-lite

Detecta:

- `fan_out`: transacoes com muitos outputs
- `whale_transaction`: transferencias acima de um threshold
- `velocity_spike`: muitas movimentacoes por endereco em janela curta
- `structuring`: repeticao de saidas pequenas em janela curta
- `late_event`: chegada apos watermark

Alertas vao para:

- Kafka topic `btc.alerts`
- ClickHouse table `alerts`

### 5. Exactly-once

- Checkpointing Flink em `EXACTLY_ONCE`
- Kafka source e Kafka sinks configurados com semantica transacional
- Estado UTXO e saldos no Flink permanecem consistentes em caso de restart
- Redis e ClickHouse usam escrita idempotente ou append-friendly para integracao operacional

## Topicos Kafka

- `btc.transactions`: eventos normalizados
- `btc.alerts`: alertas AML-lite
- `btc.metrics`: metricas e atualizacoes de balance

## Substituicao futura por Bitcoin Core

Para plugar `BitcoinCoreSource` futuramente:

1. Implementar `stream()` em `ingestion-service/app/sources/bitcoin_core.py`
2. Produzir `RawTransactionEvent`
3. Reutilizar o mesmo `normalizer.py`
4. Publicar no mesmo topic `btc.transactions`

Nada muda no Flink.

## Como subir

```bash
docker compose up --build
```

Endpoints:

- Kafka UI: `http://localhost:8080`
- Flink UI: `http://localhost:8081`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- ClickHouse HTTP: `http://localhost:8123`
- Redis: `localhost:6379`

## Escala e tuning

Parametros importantes:

- `SIM_ADDRESS_COUNT`: quantidade de enderecos simulados
- `SIM_TARGET_TPS`: taxa alvo de geracao
- `SIM_MAX_LATENESS_MS`: desordem temporal maxima
- `FLINK_PARALLELISM`: paralelismo do job
- `AML_*`: thresholds das regras AML-lite

Para atingir `10k+ tx/min`, aumente `SIM_TARGET_TPS`, particoes de `btc.transactions` e paralelismo dos taskmanagers.

## Extensibilidade

O ponto de extensao oficial esta na ingestao. O restante do pipeline depende somente do evento normalizado, preservando o contrato:

`NormalizedTransactionEvent` e a unica linguagem que o Flink entende.
