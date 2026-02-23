# Taiwan Stock Data Platform

A production-grade data pipeline that collects, stores, and serves Taiwan stock market data. Crawlers run daily via Apache Airflow, data is stored in MySQL, and exposed through a FastAPI REST API. Full observability is provided by Prometheus and Grafana.

![System Architecture](architecture.png)

---

## Architecture Overview

The platform is composed of five layers deployed on Docker Swarm, all communicating over a shared overlay network named `dev`.

**External Data Sources** — Public APIs from TWSE (Taiwan Stock Exchange), TAIFEX (Taiwan Futures Exchange), and TDCC (Securities Depository).

**Crawler** — A Dockerized Python service (`stockdata_crawler`) that fetches and parses data from the external sources. Each crawler module handles one dataset. Airflow triggers individual crawler containers via `DockerOperator`, with a 3-second delay between tasks to avoid rate limiting.

**Orchestration** — Apache Airflow 3.0 runs the DAG `taiwan_stock_data_crawler` daily at 21:00 (Asia/Taipei). Airflow uses its own PostgreSQL instance for metadata. Airflow metrics are forwarded to Prometheus via a StatsD exporter.

**Data Storage** — MySQL 8.0 is the primary datastore. phpMyAdmin provides a web management interface. A MySQL exporter exposes database metrics to Prometheus.

**API** — A FastAPI service (`stockdata-api`) reads from MySQL and exposes five REST endpoints. The service is instrumented with custom Prometheus metrics including query counters, duration histograms, and connection gauges.

**Analytics** — Redash connects directly to MySQL for ad-hoc SQL queries and dashboard building. It uses a dedicated PostgreSQL instance for metadata and Redis for its task queue.

**Monitoring** — Prometheus scrapes metrics from the API, MySQL exporter, StatsD exporter, Node Exporter (host metrics), and cAdvisor (container metrics). Grafana visualizes all metrics and dashboards.

---

## Services

| Service | Image | Port | Description |
|---|---|---|---|
| airflow | apache/airflow:3.0.3-python3.12 | 8080 | ETL scheduler and task manager |
| airflow-postgres | postgres:15 | internal | Airflow metadata database |
| statsd-exporter | prom/statsd-exporter | 9102, 8125/udp | Bridges Airflow StatsD metrics to Prometheus |
| mysql | mysql:8.0 | 3307 | Primary data store |
| phpmyadmin | phpmyadmin/phpmyadmin | 8000 | MySQL web management UI |
| mysql-exporter | prom/mysqld-exporter | 9104 | Exposes MySQL metrics for Prometheus |
| stockdata-api | stockdata_api | 8888 | FastAPI REST API |
| redash-server | redash/redash | 5000 | Analytics dashboard and SQL interface |
| redash-postgres | postgres:15 | internal | Redash metadata database |
| redash-redis | redis:7 | internal | Redash task queue |
| prometheus | prom/prometheus | 9090 | Metrics collection and alerting |
| grafana | grafana/grafana | 3000 | Monitoring dashboards |
| node-exporter | prom/node-exporter | 9100 | Host-level metrics |
| cadvisor | gcr.io/cadvisor/cadvisor | 8081 | Container-level metrics |
| portainer | — | — | Docker Swarm management UI |

---

## API Endpoints

Base URL: `http://<host>:8888`

All endpoints accept `start_date` and `end_date` in `YYYY-MM-DD` format.

| Endpoint | Query Params | Description |
|---|---|---|
| GET /taiwan_stock_price | stock_id, start_date, end_date | Daily OHLCV stock prices |
| GET /taiwan_institutional_investor | stock_id, start_date, end_date | Institutional buy/sell data |
| GET /taiwan_margin_short_sale | stock_id, start_date, end_date | Margin and short sale data |
| GET /taiwan_share_holding | stock_id, start_date, end_date | Shareholding distribution |
| GET /taiwan_future_daily | future_id, start_date, end_date | Futures daily trade data |
| GET /metrics | — | Prometheus metrics endpoint |

Example:

```bash
curl "http://localhost:8888/taiwan_stock_price?stock_id=2330&start_date=2024-01-01&end_date=2024-12-31"
```

---

## Project Structure

```
Stock_project/
├── airflow/
│   └── dags/
│       └── stock_etl_pipeline.py      # Main ETL DAG definition
├── api/
│   ├── api/
│   │   ├── main.py                    # FastAPI application and endpoints
│   │   └── config.py                  # Database connection config
│   └── Dockerfile
├── crawler/
│   └── stockdata/
│       ├── crawler/
│       │   ├── taiwan_stock_price.py
│       │   ├── taiwan_stock_info.py
│       │   ├── taiwan_institutional_investor.py
│       │   ├── taiwan_margin_short_sale.py
│       │   ├── taiwan_share_holding.py
│       │   └── taiwan_futures_daily.py
│       ├── backend/
│       │   └── db/
│       │       ├── clients.py         # SQLAlchemy / pymysql connection clients
│       │       ├── db.py              # Table definitions and upsert logic
│       │       └── router.py          # Routes dataset name to the correct DB writer
│       ├── schema/
│       │   └── dataset.py            # Pydantic models for all six datasets
│       └── main.py                    # CLI entrypoint
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alerts/
│   │       └── alerts.yml
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   └── Stockdata Monitoring-*.json    # Pre-built dashboard, auto-loaded at startup
│   │   └── provisioning/
│   │       └── datasources/
│   │           └── datasource.yml             # Registers Prometheus as default datasource
│   └── airflow/
│       └── statsd_mapping.yml                 # Maps Airflow StatsD metrics to Prometheus labels
├── stockdata_airflow.yaml
├── stockdata_database.yaml
├── stockdata_monitoring.yaml
├── stockdata_redash.yaml
├── stockdata_portainer.yaml
└── deploy.sh
```

---

## Getting Started

### Prerequisites

- Docker Engine 24+
- Docker Swarm initialized (`docker swarm init`)
- Overlay network `dev` created

```bash
docker network create --driver overlay --attachable dev
```

### 1. Configure environment variables

Create a `.env` file in the project root:

```env
# MySQL
MYSQL_DATABASE=stockdata
MYSQL_USER=stockuser
MYSQL_PASSWORD=your_password
MYSQL_ROOT_PASSWORD=your_root_password
MYSQL_HOST=mysql
MYSQL_PORT=3306

# phpMyAdmin
PMA_HOST=mysql
PMA_USER=stockuser
PMA_PASSWORD=your_password

# Airflow
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_POSTGRES_DB=airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Redash
REDASH_POSTGRES_USER=redash
REDASH_POSTGRES_PASSWORD=redash
REDASH_POSTGRES_DB=redash
REDASH_DATABASE_URL=postgresql://redash:redash@redash-postgres/redash
REDASH_COOKIE_SECRET=change_this_secret
REDASH_SECRET_KEY=change_this_key

# Grafana
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin
```

### 2. Build Docker images

```bash
# Build the crawler image
cd crawler
docker build -t stockdata_crawler:latest .

# Build the API image
cd ../api
docker build -t stockdata_api:latest .
```

### 3. Deploy all stacks

```bash
bash deploy.sh
```

Or deploy individually:

```bash
docker stack deploy -c stockdata_database.yaml   stockdata
docker stack deploy -c stockdata_airflow.yaml     stockdata
docker stack deploy -c stockdata_monitoring.yaml  stockdata
docker stack deploy -c stockdata_redash.yaml      stockdata
docker stack deploy -c stockdata_portainer.yaml   stockdata
```

### 4. Initialize Redash database

Run once after the first deployment:

```bash
docker exec -it $(docker ps -q -f name=redash-server) python manage.py database create_tables
```

---

## Database Schema

All tables are stored in MySQL 8.0. Column types follow the Pydantic models defined in `crawler/stockdata/schema/dataset.py`.

### taiwan_stock_price

| Column | Type | Description |
|---|---|---|
| StockID | VARCHAR | Stock ticker symbol |
| Date | DATE | Trading date |
| Open | FLOAT | Opening price |
| Max | FLOAT | Daily high |
| Min | FLOAT | Daily low |
| Close | FLOAT | Closing price |
| Change | FLOAT | Price change from previous close |
| TradeVolume | BIGINT | Total shares traded |
| Transaction | INT | Number of transactions |
| TradeValue | BIGINT | Total trade value in TWD |

### taiwan_stock_info

| Column | Type | Description |
|---|---|---|
| StockID | INT | Stock ticker (primary key) |
| StockName | VARCHAR | Company name |
| MarketType | VARCHAR | TWSE or TPEX |
| IndustryType | VARCHAR | Industry classification |

### taiwan_institutional_investor

| Column | Type | Description |
|---|---|---|
| StockID | VARCHAR | Stock ticker symbol |
| Date | DATE | Trading date |
| ForeignBuy / ForeignSell / ForeignNet | INT | Foreign investor buy, sell, net |
| ForeignDealerBuy / ForeignDealerSell / ForeignDealerNet | INT | Foreign dealer sub-account |
| InvestmentTrustBuy / InvestmentTrustSell / InvestmentTrustNet | INT | Investment trust (投信) |
| DealerSelfBuy / DealerSelfSell / DealerSelfNet | INT | Dealer proprietary trading |
| DealerHedgeBuy / DealerHedgeSell / DealerHedgeNet | INT | Dealer hedge account |
| ThreeInstitutionNet | INT | Combined net of all three institutions |

### taiwan_margin_short_sale

| Column | Type | Description |
|---|---|---|
| StockID | VARCHAR | Stock ticker symbol |
| Date | DATE | Trading date |
| MarginPurchaseBuy / MarginPurchaseSell | INT | Margin purchase buy and sell |
| MarginPurchaseCashRepayment | INT | Cash repayment for margin |
| MarginPurchaseYesterdayBalance / MarginPurchaseTodayBalance | INT | Margin balance change |
| MarginPurchaseLimit | INT | Margin purchase ceiling |
| ShortSaleBuy / ShortSaleSell | INT | Short sale buy and sell |
| ShortSaleCashRepayment | INT | Cash repayment for short |
| ShortSaleYesterdayBalance / ShortSaleTodayBalance | INT | Short balance change |
| ShortSaleLimit | INT | Short sale ceiling |
| OffsetLoanAndShort | INT | Offset between margin and short |

### taiwan_share_holding

| Column | Type | Description |
|---|---|---|
| Date | DATE | Record date |
| StockID | VARCHAR | Stock ticker symbol |
| ShareholdingLevel | INT | Bracket index (1-15, grouped by share count range) |
| NumberOfHolders | INT | Number of shareholders in this bracket |
| NumberOfShares | INT | Total shares held in this bracket |
| PercentageOfTotalShares | FLOAT | Percentage of total outstanding shares |

### taiwan_futures_daily

| Column | Type | Description |
|---|---|---|
| Date | DATE | Trading date |
| FuturesID | VARCHAR | Futures contract identifier |
| ContractDate | VARCHAR | Delivery month |
| Open / Max / Min / Close | FLOAT | OHLC prices |
| Change / ChangePer | FLOAT | Price change and percentage change |
| Volume | FLOAT | Total traded volume |
| SettlementPrice | FLOAT | Daily settlement price |
| OpenInterest | INT | Open interest at end of day |
| TradingSession | VARCHAR | Regular or after-hours session |

---

## Monitoring

Prometheus scrape targets:

| Target | Port | Metrics |
|---|---|---|
| stockdata-api | 8888 | HTTP request rate, latency, DB query duration, active connections |
| mysql-exporter | 9104 | MySQL connections, query throughput, table sizes |
| statsd-exporter | 9102 | Airflow DAG duration, task success/failure counts |
| node-exporter | 9100 | Host CPU, memory, disk, network |
| cadvisor | 8081 | Per-container CPU, memory, network I/O |

Alert rules are defined in `monitoring/prometheus/alerts/alerts.yml`.

**Grafana provisioning** is handled automatically at container startup via two directories mounted into the Grafana container:

`monitoring/grafana/provisioning/datasources/datasource.yml` — registers Prometheus as the default datasource:

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    jsonData:
      timeInterval: 15s
      queryTimeout: 60s
```

`monitoring/grafana/dashboards/` — contains pre-built dashboard JSON files that Grafana loads on startup. The included dashboard (`Stockdata Monitoring-*.json`) covers API request rates, DB query durations, active connections, and error counts. To add a new dashboard, export it from the Grafana UI as JSON and drop the file into this directory, then redeploy the Grafana service.

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.12 |
| API Framework | FastAPI |
| ETL Scheduler | Apache Airflow 3.0 |
| Primary Database | MySQL 8.0 |
| Metadata Databases | PostgreSQL 15 |
| Task Queue | Redis 7 |
| Metrics and Alerting | Prometheus |
| Visualization | Grafana, Redash |
| Container Runtime | Docker Swarm |

---

## License

MIT
