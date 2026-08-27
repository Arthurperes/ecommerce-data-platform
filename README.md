# 🛒 E-commerce Data Platform

Plataforma de Engenharia de Dados para processamento e análise de eventos de comportamento de usuários em um ambiente de e-commerce, com foco na identificação de padrões relacionados ao **abandono de carrinho** e na estimativa de **receita potencial não capturada**.

O projeto utiliza conceitos de **Big Data, Data Lake, Arquitetura Lambda, Arquitetura Medallion, processamento distribuído, streaming, Data Quality, Machine Learning, Infrastructure as Code e CI/CD**.

> 🚧 **Status:** Em desenvolvimento

---

## 🎯 Problema de Negócio

O abandono de carrinho representa uma perda relevante de receita para plataformas de e-commerce.

O objetivo deste projeto é utilizar eventos de navegação para responder à seguinte questão:

> **Quais padrões comportamentais e fatores de navegação estão associados ao abandono de carrinho e qual é o potencial de receita não capturada decorrente das compras não concluídas?**

A plataforma processa eventos como:

* `view` — visualização de produto
* `cart` — adição ao carrinho
* `purchase` — compra realizada

A partir desses eventos será possível reconstruir jornadas de usuários, analisar o funil de conversão e desenvolver modelos capazes de identificar sessões com maior probabilidade de abandono.

---

## 🏗️ Arquitetura

A solução combina conceitos de **Lambda Architecture** e **Medallion Architecture**.

```text
                         E-COMMERCE EVENTS
                                │
                 ┌──────────────┴──────────────┐
                 │                             │
           BATCH LAYER                    SPEED LAYER
                 │                             │
        Historical Dataset              Python Producer
                 │                             │
                 │                           Kafka
                 │                             │
                 │                         Consumer
                 │                             │
                 └──────────────┬──────────────┘
                                │
                                ▼
                              MinIO
                            DATA LAKE
                                │
                  ┌─────────────┼─────────────┐
                  │             │             │
                  ▼             ▼             ▼
               BRONZE        SILVER          GOLD
                RAW          TRUSTED        CURATED
                  │             ▲             │
                  │             │             │
                  └─────────► PySpark ◄───────┘
                                              │
                              ┌───────────────┼──────────────┐
                              │               │              │
                              ▼               ▼              ▼
                         PostgreSQL      Wide Table          ML
                              │                              │
                              └───────────────┬──────────────┘
                                              ▼
                                          Streamlit
                                          Dashboard
```

---

## 🧱 Arquitetura Medallion

### 🥉 Bronze — Raw

A camada Bronze preserva os dados históricos em seu formato original.

```text
2019-Nov.csv
      │
      ▼
batch_ingestion.py
      │
      ▼
MinIO
      │
      ▼
bronze/ecommerce_events/year=2019/month=11/
```

Nenhuma transformação relevante é realizada nesta etapa, permitindo rastreabilidade e reprocessamento.

### 🥈 Silver — Trusted

A camada Silver utiliza **PySpark** para transformar e validar os eventos da Bronze.

Entre os tratamentos previstos/implementados estão:

* tipagem de dados;
* padronização dos eventos;
* validação de timestamps;
* validação de identificadores;
* validação de preços;
* remoção de registros inválidos;
* remoção de duplicidades;
* regras de Data Quality;
* armazenamento em formato Parquet.

O dataset utilizado possui aproximadamente **67 milhões de eventos**, permitindo trabalhar com processamento distribuído em um cenário de volume significativo.

### 🥇 Gold — Curated

A camada Gold será responsável por disponibilizar informações orientadas ao negócio.

Entre os datasets planejados:

```text
funnel_metrics
abandoned_carts
session_features
revenue_metrics
category_metrics
```

Essa camada servirá de origem para análises, dashboard, modelo dimensional e Machine Learning.

---

## ⚡ Lambda Architecture

O projeto combina dois caminhos de processamento.

### Batch Layer

Responsável pelo processamento do dataset histórico:

```text
CSV → Python → MinIO Bronze → PySpark → Silver → Gold
```

### Speed Layer

Responsável pela simulação de eventos chegando continuamente:

```text
Events → Python Producer → Kafka → Consumer → Data Lake
```

O objetivo é demonstrar como a mesma plataforma poderia processar dados históricos e eventos próximos do tempo real.

---

## 📊 Dataset

O projeto utiliza uma base pública de comportamento de usuários em e-commerce.

Principais atributos:

| Campo           | Descrição                                   |
| --------------- | ------------------------------------------- |
| `event_time`    | Data e horário do evento                    |
| `event_type`    | Tipo do evento (`view`, `cart`, `purchase`) |
| `product_id`    | Identificador do produto                    |
| `category_id`   | Identificador da categoria                  |
| `category_code` | Categoria hierárquica do produto            |
| `brand`         | Marca                                       |
| `price`         | Preço do produto                            |
| `user_id`       | Identificador do usuário                    |
| `user_session`  | Identificador da sessão                     |

O arquivo histórico utilizado localmente possui aproximadamente **9 GB** e dezenas de milhões de eventos.

Por questões de tamanho, o dataset **não é versionado neste repositório**.

---

## 🔄 Pipeline de Dados

O pipeline é dividido em etapas independentes:

```text
1. Ingestão Batch
       ↓
2. Bronze / Raw
       ↓
3. Processamento PySpark
       ↓
4. Data Quality
       ↓
5. Silver / Trusted
       ↓
6. Gold / Curated
       ↓
7. Star Schema
       ↓
8. Wide Table
       ↓
9. Machine Learning
       ↓
10. Dashboard Streamlit
```

Paralelamente, Kafka será utilizado para implementação da Speed Layer.

---

## 🤖 Machine Learning

Uma das etapas planejadas é a construção de um modelo classificatório para identificar sessões com maior probabilidade de abandono.

Exemplo de variável alvo:

```text
abandoned_cart

0 → compra concluída
1 → carrinho abandonado
```

Features comportamentais poderão incluir:

```text
total_views
total_cart_events
unique_products
unique_categories
average_price
maximum_price
session_duration
cart_value
```

Modelos candidatos:

* Logistic Regression
* Decision Tree
* Random Forest

A avaliação considerará métricas adequadas ao problema de classificação e possíveis desbalanceamentos entre as classes.

---

## ⭐ Star Schema

A camada analítica utilizará modelagem dimensional para facilitar consultas e análises.

Estrutura planejada:

```text
                    DIM_DATE
                       │
                       │
DIM_USER ──────── FACT_EVENT ──────── DIM_PRODUCT
                       │
                       │
                 DIM_CATEGORY
```

O modelo será utilizado para análises de eventos, usuários, produtos, categorias, conversão e receita.

---

## 🧠 Wide Table

Além do modelo dimensional, será construída uma **Wide Table** orientada ao Machine Learning.

Cada registro poderá representar uma sessão de usuário contendo features comportamentais consolidadas.

Exemplo:

```text
user_session
user_id
total_views
total_cart_events
unique_products
unique_categories
avg_price
max_price
session_duration
cart_value
purchase
abandoned_cart
```

---

## 📈 Dashboard

O dashboard será desenvolvido utilizando **Streamlit**.

Indicadores planejados:

### Visão geral

* total de eventos;
* usuários;
* sessões;
* produtos;
* receita.

### Funil de conversão

```text
VIEW
  ↓
CART
  ↓
PURCHASE
```

### Abandono de carrinho

* taxa de abandono;
* quantidade de carrinhos abandonados;
* valor potencial perdido;
* categorias com maior abandono;
* marcas com maior abandono.

### Machine Learning

* desempenho do modelo;
* probabilidade de abandono;
* features mais relevantes.

---

## 🛠️ Tecnologias

| Categoria                 | Tecnologia               |
| ------------------------- | -----------------------  |
| Linguagem                 | Python                   |
| Processamento distribuído | Apache Spark / PySpark   | 
| Streaming                 | Apache Kafka             |
| Data Lake                 | S3                       |
| Banco analítico           | PostgreSQL               |
| Administração             | pgAdmin                  |
| Dashboard                 | Streamlit / PWB -- "TBD" |
| Machine Learning          | Scikit-learn  - "TBD"    |
| Containers                | Docker / Docker Compose  |
| Infrastructure as Code    | Terraform                |
| CI/CD                     | GitHub Actions           |
| Versionamento             | Git / GitHub             |

---

## 🐳 Infraestrutura Docker

O ambiente local é executado utilizando Docker Compose.

Serviços:

```text
Kafka
Zookeeper
MinIO
PostgreSQL
pgAdmin
Spark
```

Para iniciar o ambiente:

```bash
docker compose up -d
```

Verifique os containers:

```bash
docker compose ps
```

Para encerrar:

```bash
docker compose down
```

> Não utilize `docker compose down -v` caso queira preservar os volumes persistentes.

---

## 📂 Estrutura do Projeto

```text
ecommerce-data-platform/
│
├── app/
│   ├── batch_ingestion.py
│   ├── ecommerce_spark_processor.py
│   ├── producer_*.py
│   ├── consumer.py
│   └── ...
│
├── dashboard/
│   └── streamlit_app.py
│
├── data/
│   └── input/
│       └── 2019-Nov.csv
│
├── sql/
│   └── create_tables.sql
│
├── terraform/
│   └── main.tf
│
├── tests/
│
├── docs/
│
├── .github/
│   └── workflows/
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

A pasta `data/` é ignorada pelo Git para evitar o versionamento de arquivos de grande volume.

---

## 🚀 Executando o Projeto

### 1. Clone o repositório

```bash
git clone https://github.com/Arthurperes/ecommerce-data-platform.git
cd ecommerce-data-platform
```

### 2. Crie o ambiente Python

Recomendado:

```text
Python 3.12
Java 17
```

No Windows:

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
python -m pip install -r requirements.txt
```

### 4. Inicie a infraestrutura

```bash
docker compose up -d
```

### 5. Execute a ingestão Batch

Coloque o dataset em:

```text
data/input/2019-Nov.csv
```

Execute:

```bash
python app/batch_ingestion.py
```

O arquivo será enviado para:

```text
MinIO
└── bronze
    └── ecommerce_events
        └── year=2019
            └── month=11
```

---

## 🔍 Data Quality

A plataforma incorpora validações durante a transformação dos dados.

Exemplos:

```text
event_time IS NOT NULL
event_type IN ('view', 'cart', 'purchase')
product_id IS NOT NULL
user_id IS NOT NULL
price >= 0
duplicate detection
schema validation
```

As regras serão evoluídas juntamente com as camadas Silver e Gold.

---

## ⚙️ CI/CD

O pipeline de CI/CD será implementado utilizando **GitHub Actions**.

Fluxo planejado:

```text
Push / Pull Request
        │
        ▼
Python Setup
        │
        ▼
Dependencies
        │
        ▼
Lint
        │
        ▼
Tests
        │
        ▼
Terraform Validation
        │
        ▼
Docker Validation
```

---

## 🏗️ Infrastructure as Code

A infraestrutura do projeto será versionada utilizando **Terraform**, permitindo demonstrar conceitos de Infrastructure as Code (IaC), reprodutibilidade e automação do ambiente.

---

## 🔐 Governança e Segurança

O projeto considera práticas relacionadas a:

* separação entre dados Raw, Trusted e Curated;
* rastreabilidade dos dados;
* controle de acesso;
* Data Quality;
* versionamento;
* proteção de identificadores de usuários;
* princípios relacionados à LGPD;
* documentação das transformações.

---

## 🗺️ Roadmap

* [x] Definição do problema de negócio
* [x] Seleção do dataset
* [x] Arquitetura Lambda
* [x] Ambiente Docker
* [x] Kafka e Zookeeper
* [x] MinIO
* [x] PostgreSQL e pgAdmin
* [x] Ingestão Batch
* [x] Bronze Layer
* [x] Ambiente PySpark
* [ ] Silver Layer
* [ ] Data Quality completo
* [ ] Gold Layer
* [ ] Star Schema
* [ ] Wide Table
* [ ] EDA
* [ ] Machine Learning
* [ ] Speed Layer com Kafka
* [ ] Dashboard Streamlit
* [ ] Terraform
* [ ] GitHub Actions
* [ ] Testes automatizados
* [ ] Documentação final

---

## 📌 Status Atual

Atualmente, a plataforma possui infraestrutura local containerizada e ingestão Batch funcional.

O dataset histórico já pode ser enviado para a camada **Bronze no MinIO**, e o pipeline **Bronze → Silver utilizando PySpark** encontra-se em desenvolvimento.

O objetivo das próximas etapas é concluir:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Star Schema + Wide Table
   ↓
ML + Streamlit
```

---

## 👨‍💻 Autores

**Arthur Peres**
**João Vitor**
**Maria Luiza**
**Paulo Vasconcelos**
**Bianca**

Projeto desenvolvido como parte do **Hands-On de Engenharia de Dados — Universidade Presbiteriana Mackenzie**.

---

## 📄 Licença

Projeto desenvolvido para fins acadêmicos e de estudo.
