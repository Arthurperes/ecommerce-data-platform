import os
import sys
from textwrap import dedent

import pandas as pd
import streamlit as st
from pyathena import connect


# ============================================================
# PATH DO PROJETO
# ============================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        ".."
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.decision_matrix import get_action


# ============================================================
# CONFIGURACAO AWS / ATHENA
# ============================================================

AWS_REGION = "us-east-1"

ATHENA_STAGING_DIR = (
    "s3://ecommerce-data-platform-mack-lab/"
    "athena-results/"
)

ATHENA_DATABASE = "ecommerce_data_platform"

SESSION_TABLE = "gold_glue_test_session_features"
FUNNEL_TABLE = "gold_glue_test_funnel_metrics"


@st.cache_data(ttl=300)
def run_athena_query(sql):
    conn = connect(
        s3_staging_dir=ATHENA_STAGING_DIR,
        region_name=AWS_REGION
    )

    return pd.read_sql_query(
        sql,
        conn
    )


# ============================================================
# CONFIGURACAO DA PAGINA
# ============================================================

st.set_page_config(
    page_title="E-commerce Cart Recovery",
    page_icon="🛒",
    layout="wide"
)


# ============================================================
# CSS
# ============================================================

st.markdown(
    """
    <style>

    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1500px;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(
            180deg,
            #0f172a 0%,
            #111827 100%
        );
    }

    [data-testid="stSidebar"] * {
        color: #f8fafc;
    }

    [data-testid="stSidebar"] hr {
        border-color: #334155;
    }

    h1 {
        font-weight: 750;
        letter-spacing: -0.8px;
    }

    h2, h3 {
        font-weight: 650;
        letter-spacing: -0.4px;
    }

    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        padding: 18px 20px;
        border-radius: 14px;
        box-shadow:
            0 2px 6px rgba(15, 23, 42, 0.04),
            0 8px 20px rgba(15, 23, 42, 0.04);
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.88rem;
        color: #64748b;
        font-weight: 500;
    }

    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #0f172a;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        overflow: hidden;
    }

    [data-testid="stAlert"] {
        border-radius: 12px;
    }

    hr {
        margin-top: 2rem;
        margin-bottom: 2rem;
        border-color: #e5e7eb;
    }

    .hero {
        padding: 26px 30px;
        border-radius: 18px;
        margin-bottom: 28px;
        background:
            linear-gradient(
                135deg,
                #0f172a 0%,
                #1e293b 60%,
                #0f766e 100%
            );
        color: white;
        box-shadow: 0 12px 35px rgba(15, 23, 42, 0.15);
    }

    .hero h1 {
        margin: 0;
        color: white;
        font-size: 2.15rem;
    }

    .hero p {
        margin-top: 8px;
        margin-bottom: 0;
        color: #cbd5e1;
        font-size: 1rem;
    }

    .badge {
        display: inline-block;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(255,255,255,0.12);
        color: #e2e8f0;
        font-size: 0.78rem;
        margin-right: 6px;
        margin-top: 12px;
    }

    .pipeline-container {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        margin: 20px 0 30px 0;
        flex-wrap: wrap;
    }

    .pipeline-card {
        flex: 1;
        min-width: 120px;
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 16px 10px;
        text-align: center;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.05);
    }

    .pipeline-icon {
        font-size: 1.65rem;
        margin-bottom: 6px;
    }

    .pipeline-title {
        font-weight: 700;
        color: #0f172a;
        font-size: 0.95rem;
    }

    .pipeline-subtitle {
        font-size: 0.76rem;
        color: #64748b;
        margin-top: 4px;
    }

    .pipeline-arrow {
        font-size: 1.4rem;
        color: #64748b;
    }

    .status-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px 16px;
        min-height: 105px;
    }

    .status-ok {
        color: #15803d;
        font-weight: 700;
    }

    .model-winner {
        background: linear-gradient(
            135deg,
            #ecfdf5,
            #f0fdfa
        );
        border: 1px solid #a7f3d0;
        border-radius: 14px;
        padding: 18px;
        margin-bottom: 20px;
    }

    #MainMenu {
        visibility: hidden;
    }

    footer {
        visibility: hidden;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ============================================================
# CAMINHOS LOCAIS
# ============================================================

CLUSTER_CONVERSION_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "cluster_conversion.csv"
)

CLUSTER_SUMMARY_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "cluster_summary.csv"
)

CLUSTER_METRICS_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "clustering_metrics.csv"
)


# ============================================================
# PERFIS
# ============================================================

PROFILE_MAP = {
    0: "Navegador Indeciso",
    1: "Comprador de Alta Intencao",
    2: "Cacador de Descontos"
}


# ============================================================
# CARGA CSV
# ============================================================

@st.cache_data
def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)

    return pd.DataFrame()


cluster_conversion_df = load_csv(
    CLUSTER_CONVERSION_PATH
)

cluster_summary_df = load_csv(
    CLUSTER_SUMMARY_PATH
)

cluster_metrics_df = load_csv(
    CLUSTER_METRICS_PATH
)


# ============================================================
# CABECALHO
# ============================================================

st.markdown(
    dedent(
        """
        <div class="hero">
        <h1>🛒 E-commerce Cart Recovery</h1>

        <p>
            Plataforma analítica para identificação de abandono
            de carrinho, propensão à conversão e recomendação de
            ações comerciais.
        </p>

        <span class="badge">AWS</span>
        <span class="badge">S3 Data Lake</span>
        <span class="badge">AWS Glue</span>
        <span class="badge">PySpark</span>
        <span class="badge">Athena</span>
        <span class="badge">Spark ML</span>
        <span class="badge">EC2</span>
        <span class="badge">Terraform</span>
            </div>
        """
    ),
    unsafe_allow_html=True
)

# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.markdown(
    """
    ## 🛒 Cart Recovery

    **E-commerce Data Platform**

    ---
    """
)

page = st.sidebar.radio(
    "Selecione uma visão",
    [
        "Visão Geral",
        "Modelo de Propensão",
        "Perfis de Clientes",
        "Matriz de Decisão",
        "Simulador"
    ]
)

st.sidebar.markdown("---")

st.sidebar.caption(
    "AWS • Glue • Athena • Spark ML • EC2"
)


# ============================================================
# VISAO GERAL
# ============================================================

if page == "Visão Geral":

    st.header(
        "📊 Visão Geral do Projeto"
    )

    st.markdown(
    	'<div class="pipeline-container">'
    	'<div class="pipeline-card"><div class="pipeline-icon">📄</div><div class="pipeline-title">Dataset</div><div class="pipeline-subtitle">Eventos E-commerce</div></div>'
    	'<div class="pipeline-arrow">→</div>'
  	'<div class="pipeline-card"><div class="pipeline-icon">🪣</div><div class="pipeline-title">S3 Bronze</div><div class="pipeline-subtitle">Raw / CSV</div></div>'
    	'<div class="pipeline-arrow">→</div>'
    	'<div class="pipeline-card"><div class="pipeline-icon">⚙️</div><div class="pipeline-title">AWS Glue</div><div class="pipeline-subtitle">PySpark ETL</div></div>'
    	'<div class="pipeline-arrow">→</div>'
    	'<div class="pipeline-card"><div class="pipeline-icon">🥈</div><div class="pipeline-title">Silver</div><div class="pipeline-subtitle">Parquet tratado</div></div>'
    	'<div class="pipeline-arrow">→</div>'
    	'<div class="pipeline-card"><div class="pipeline-icon">🥇</div><div class="pipeline-title">Gold</div><div class="pipeline-subtitle">Features / Métricas</div></div>'
    	'<div class="pipeline-arrow">→</div>'
    	'<div class="pipeline-card"><div class="pipeline-icon">🔎</div><div class="pipeline-title">Athena</div><div class="pipeline-subtitle">SQL Analytics</div></div>'
    	'<div class="pipeline-arrow">→</div>'
    	'<div class="pipeline-card"><div class="pipeline-icon">📊</div><div class="pipeline-title">EC2</div><div class="pipeline-subtitle">Streamlit</div></div>'
    	'</div>',
    	unsafe_allow_html=True,
     )       

    try:

        overview_query = f"""
        SELECT
            COUNT(*) AS cart_sessions,
            SUM(
                CASE
                    WHEN is_abandoned = 1
                    THEN 1
                    ELSE 0
                END
            ) AS abandoned_sessions,
            SUM(
                CASE
                    WHEN converted = 1
                    THEN 1
                    ELSE 0
                END
            ) AS converted_sessions
        FROM {ATHENA_DATABASE}.{SESSION_TABLE}
        """

        funnel_query = f"""
        SELECT
            event_type,
            count
        FROM {ATHENA_DATABASE}.{FUNNEL_TABLE}
        """

        overview_df = run_athena_query(
            overview_query
        )

        funnel_df = run_athena_query(
            funnel_query
        )

        cart_sessions = int(
            overview_df.iloc[0]["cart_sessions"]
        )

        abandoned_sessions = int(
            overview_df.iloc[0]["abandoned_sessions"]
        )

        converted_sessions = int(
            overview_df.iloc[0]["converted_sessions"]
        )

        total_events = int(
            funnel_df["count"].sum()
        )

        abandonment_rate = (
            abandoned_sessions
            / cart_sessions
            * 100
        )

        conversion_rate = (
            converted_sessions
            / cart_sessions
            * 100
        )

        # ----------------------------------------------------
        # KPIs PRINCIPAIS
        # ----------------------------------------------------

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Eventos processados",
            f"{total_events:,}".replace(",", ".")
        )

        col2.metric(
            "Sessões com carrinho",
            f"{cart_sessions:,}".replace(",", ".")
        )

        col3.metric(
            "Carrinhos abandonados",
            f"{abandoned_sessions:,}".replace(",", ".")
        )

        col4.metric(
            "Sessões convertidas",
            f"{converted_sessions:,}".replace(",", ".")
        )

        st.caption(
            "Dados carregados diretamente do Amazon Athena "
            "sobre a camada Gold processada no AWS Glue."
        )

        st.divider()

        # ----------------------------------------------------
        # RESUMO EXECUTIVO
        # ----------------------------------------------------

        st.subheader(
            "Resumo Executivo"
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Taxa de abandono",
            f"{abandonment_rate:.2f}%"
        )

        col2.metric(
            "Taxa de conversão",
            f"{conversion_rate:.2f}%"
        )

        col3.metric(
            "Modelo ML",
            "GBT V3"
        )

        col4.metric(
            "ROC-AUC",
            "0.718"
        )

        st.caption(
            "Pipeline executado na AWS utilizando S3, Glue, "
            "Spark ML, Glue Data Catalog, Athena e EC2."
        )

        st.divider()

        # ----------------------------------------------------
        # ABANDONO E CONVERSAO
        # ----------------------------------------------------

        st.subheader(
            "Taxa de abandono e conversão"
        )

        conversion_summary = pd.DataFrame(
            {
                "Status": [
                    "Abandonado",
                    "Convertido"
                ],
                "Quantidade": [
                    abandoned_sessions,
                    converted_sessions
                ]
            }
        )

        st.bar_chart(
            conversion_summary.set_index(
                "Status"
            )
        )

        st.divider()

        # ----------------------------------------------------
        # FUNIL
        # ----------------------------------------------------

        st.subheader(
            "Funil de Eventos"
        )

        st.bar_chart(
            funnel_df.set_index(
                "event_type"
            )
        )

        st.dataframe(
            funnel_df,
            use_container_width=True,
            hide_index=True
        )

        funnel_values = (
            funnel_df
            .set_index("event_type")["count"]
            .to_dict()
        )

        views = funnel_values.get(
            "view",
            0
        )

        carts = funnel_values.get(
            "cart",
            0
        )

        purchases = funnel_values.get(
            "purchase",
            0
        )

        view_to_cart = (
            carts / views * 100
            if views > 0
            else 0
        )

        cart_to_purchase = (
            purchases / carts * 100
            if carts > 0
            else 0
        )

        view_to_purchase = (
            purchases / views * 100
            if views > 0
            else 0
        )

        st.subheader(
            "Conversão entre etapas do funil"
        )

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "View → Cart",
            f"{view_to_cart:.2f}%"
        )

        col2.metric(
            "Cart → Purchase",
            f"{cart_to_purchase:.2f}%"
        )

        col3.metric(
            "View → Purchase",
            f"{view_to_purchase:.2f}%"
        )

        st.divider()

        # ----------------------------------------------------
        # STATUS DA PLATAFORMA
        # ----------------------------------------------------

        st.subheader(
            "☁️ Status da Plataforma"
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                """
                <div class="status-card">
                    <b>Amazon S3</b><br>
                    <span class="status-ok">● Ativo</span><br>
                    <small>Bronze / Silver / Gold</small>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col2:
            st.markdown(
                """
                <div class="status-card">
                    <b>AWS Glue</b><br>
                    <span class="status-ok">● Processado</span><br>
                    <small>ETL + ML V2</small>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col3:
            st.markdown(
                """
                <div class="status-card">
                    <b>Amazon Athena</b><br>
                    <span class="status-ok">● Conectado</span><br>
                    <small>Consultas Gold</small>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col4:
            st.markdown(
                """
                <div class="status-card">
                    <b>Amazon EC2</b><br>
                    <span class="status-ok">● Online</span><br>
                    <small>Streamlit :8501</small>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.info(
            "A visão executiva consulta a camada Gold por meio "
            "do Amazon Athena e do AWS Glue Data Catalog."
        )

    except Exception as e:

        st.error(
            "Erro ao consultar o Amazon Athena."
        )

        st.code(
            str(e)
        )


# ============================================================
# MODELO DE PROPENSAO
# ============================================================

elif page == "Modelo de Propensão":

    st.header(
        "🤖 Modelo de Propensão à Conversão"
    )

    st.write(
        "A versão final foi treinada na AWS utilizando "
        "Apache Spark ML. Random Forest e Gradient-Boosted "
        "Trees foram comparados na validação, utilizando "
        "32 features comportamentais e históricas."
    )

    st.markdown(
        """
        <div class="model-winner">
            <b>🏆 Modelo final: GBTClassifier V3</b><br>
            Selecionado pelo melhor desempenho de ROC-AUC
            na validação.
        </div>
        """,
        unsafe_allow_html=True
    )

    st.subheader(
        "Resultados do Modelo Final - V3"
    )

    col1, col2, col3 = st.columns(3)

    col1.metric("Accuracy", "69.71%")
    col2.metric("Precision", "66.95%")
    col3.metric("Recall", "36.68%")

    col4, col5, col6 = st.columns(3)

    col4.metric("F1 Score", "47.40%")
    col5.metric("ROC-AUC", "0.718")
    col6.metric("PR-AUC", "0.617")

    st.caption(
        "Treinamento e avaliação executados no AWS Glue "
        "com Apache Spark ML. O V3 utiliza 32 features, "
        "incluindo histórico anterior do usuário."
    )

    st.divider()

    st.subheader(
        "Evolução do Modelo"
    )

    comparison_df = pd.DataFrame(
        {
            "Métrica": [
                "Accuracy",
                "Precision",
                "Recall",
                "F1 Score",
                "ROC-AUC"
            ],
            "V1 - Random Forest": [
                0.5809,
                0.4538,
                0.5943,
                0.5146,
                0.6320
            ],
            "V2 - GBT": [
                0.645769,
                0.562531,
                0.207932,
                0.303631,
                0.644025
            ],
            "V3 - GBT": [
                0.697099,
                0.669453,
                0.366827,
                0.473952,
                0.717878
            ]
        }
    )

    st.dataframe(
        comparison_df,
        use_container_width=True,
        hide_index=True
    )

    st.bar_chart(
        comparison_df.set_index("Métrica")
    )

    st.success(
        "A V3 apresentou evolução consistente em relação à V2. "
        "O ROC-AUC no conjunto de teste passou de 0,644 para 0,718, "
        "e o PR-AUC subiu de aproximadamente 0,501 para 0,617. "
        "O ganho foi obtido principalmente com a inclusão de "
        "features históricas do usuário."
    )

    st.divider()

    st.subheader(
        "Comparação dos Modelos na Validação - V3"
    )

    validation_df = pd.DataFrame(
        {
            "Modelo": [
                "Random Forest V3",
                "GBT V3"
            ],
            "ROC-AUC": [
                0.701022,
                0.716835
            ],
            "PR-AUC": [
                0.600414,
                0.615021
            ]
        }
    )

    st.dataframe(
        validation_df,
        use_container_width=True,
        hide_index=True
    )

    st.bar_chart(
        validation_df.set_index("Modelo")
    )

    st.info(
        "O GBT V3 foi selecionado como modelo final porque "
        "apresentou o melhor ROC-AUC e PR-AUC na validação."
    )

    st.divider()

    st.subheader(
        "Features Históricas Adicionadas no V3"
    )

    history_features_df = pd.DataFrame(
        {
            "Feature": [
                "user_prior_sessions",
                "user_prior_conversions",
                "user_prior_abandonments",
                "user_prior_conversion_rate",
                "user_prior_abandon_rate",
                "user_lifetime_hours"
            ],
            "Descrição": [
                "Quantidade de sessões anteriores do usuário",
                "Conversões anteriores do usuário",
                "Abandonos anteriores do usuário",
                "Taxa histórica de conversão",
                "Taxa histórica de abandono",
                "Tempo de relacionamento observado do usuário"
            ]
        }
    )

    st.dataframe(
        history_features_df,
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        "As features históricas utilizam apenas sessões anteriores "
        "à sessão avaliada, reduzindo risco de data leakage."
    )

    st.divider()

    st.subheader(
        "Interpretação do Resultado"
    )

    st.write(
        "O ROC-AUC de aproximadamente 0,718 indica que o V3 "
        "possui capacidade de discriminação superior às versões "
        "anteriores. O modelo ainda não representa uma solução "
        "de produção, mas é adequado como MVP analítico para "
        "priorização de carrinhos e apoio à matriz de decisão."
    )


# ============================================================
elif page == "Perfis de Clientes":

    st.header(
        "👥 Segmentação Comportamental"
    )

    st.write(
        "O K-Means agrupa sessões com comportamentos "
        "semelhantes sem utilizar o target de conversão "
        "durante o processo de clustering."
    )

    if not cluster_metrics_df.empty:

        silhouette = cluster_metrics_df.iloc[
            0
        ][
            "silhouette_score"
        ]

        st.metric(
            "Silhouette Score",
            f"{silhouette:.4f}"
        )

    if not cluster_conversion_df.empty:

        clusters = (
            cluster_conversion_df.copy()
        )

        clusters[
            "Perfil"
        ] = clusters[
            "cluster"
        ].map(
            PROFILE_MAP
        )

        st.subheader(
            "Conversão por Perfil"
        )

        st.dataframe(
            clusters[
                [
                    "Perfil",
                    "sessions",
                    "conversion_rate",
                    "abandonment_rate"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )

        chart_df = (
            clusters[
                [
                    "Perfil",
                    "conversion_rate"
                ]
            ]
            .set_index(
                "Perfil"
            )
        )

        st.bar_chart(
            chart_df
        )

    st.divider()

    st.subheader(
        "Interpretação dos Perfis"
    )

    st.markdown(
        """
### 🎯 Comprador de Alta Intenção
Poucas visualizações antes do carrinho e decisão mais rápida.

### 🔍 Navegador Indeciso
Maior volume de visualizações, mais produtos analisados
e maior tempo antes da decisão.

### 🤑 Caçador de Descontos
Segmento interpretado como potencialmente mais sensível
a incentivos comerciais, cupons e frete.
"""
    )

    if not cluster_summary_df.empty:

        st.subheader(
            "Médias das Features por Cluster"
        )

        st.dataframe(
            cluster_summary_df,
            use_container_width=True,
            hide_index=True
        )

    st.caption(
        "Os nomes dos perfis são interpretações de negócio "
        "dos clusters encontrados pelo algoritmo."
    )


# ============================================================
# MATRIZ DE DECISAO
# ============================================================

elif page == "Matriz de Decisão":

    st.header(
        "🎯 Matriz Perfil × Score × Ação"
    )

    st.write(
        "A camada de decisão combina perfil comportamental "
        "e score de propensão para escolher a ação comercial."
    )

    matrix_data = pd.DataFrame(
        [
            {
                "Perfil": "Comprador de Alta Intencao",
                "Score": "0% a 25%",
                "Faixa": "Baixa",
                "Ação": "Notificação Simples"
            },
            {
                "Perfil": "Comprador de Alta Intencao",
                "Score": "25% a 75%",
                "Faixa": "Moderada",
                "Ação": "Notificação de Escassez"
            },
            {
                "Perfil": "Comprador de Alta Intencao",
                "Score": "75% a 100%",
                "Faixa": "Alta",
                "Ação": "Frete Grátis"
            },
            {
                "Perfil": "Navegador Indeciso",
                "Score": "0% a 25%",
                "Faixa": "Baixa",
                "Ação": "Prova Social"
            },
            {
                "Perfil": "Navegador Indeciso",
                "Score": "25% a 75%",
                "Faixa": "Moderada",
                "Ação": "Frete Grátis"
            },
            {
                "Perfil": "Navegador Indeciso",
                "Score": "75% a 100%",
                "Faixa": "Alta",
                "Ação": "Frete Grátis + 5% OFF"
            },
            {
                "Perfil": "Cacador de Descontos",
                "Score": "0% a 25%",
                "Faixa": "Baixa",
                "Ação": "Moedas / Pontos"
            },
            {
                "Perfil": "Cacador de Descontos",
                "Score": "25% a 75%",
                "Faixa": "Moderada",
                "Ação": "Cupom 5% OFF ou Frete"
            },
            {
                "Perfil": "Cacador de Descontos",
                "Score": "75% a 100%",
                "Faixa": "Alta",
                "Ação": "Frete Grátis + 10% a 15% OFF"
            }
        ]
    )

    st.dataframe(
        matrix_data,
        use_container_width=True,
        hide_index=True
    )

    st.info(
        "A matriz de decisão é uma camada de regra de negócio "
        "aplicada após os modelos de propensão e segmentação."
    )


# ============================================================
# SIMULADOR
# ============================================================

elif page == "Simulador":

    st.header(
        "🧪 Simulador de Recomendação"
    )

    st.write(
        "Simule um cliente informando perfil comportamental "
        "e score de propensão para visualizar a ação sugerida."
    )

    profile = st.selectbox(
        "Perfil do cliente",
        [
            "Comprador de Alta Intencao",
            "Navegador Indeciso",
            "Cacador de Descontos"
        ]
    )

    score_percent = st.slider(
        "Score de propensão à conversão",
        min_value=0,
        max_value=100,
        value=55,
        step=1
    )

    score = (
        score_percent
        / 100
    )

    decision = get_action(
        profile,
        score
    )

    st.divider()

    col1, col2 = st.columns(
        2
    )

    col1.metric(
        "Conversion Score",
        f"{score:.0%}"
    )

    col2.metric(
        "Faixa",
        decision[
            "score_band"
        ]
    )

    st.subheader(
        "Ação Recomendada"
    )

    st.success(
        decision[
            "action"
        ]
    )

    st.write(
        "**Benefício:**",
        decision[
            "benefit"
        ]
    )

    st.write(
        "**Mensagem sugerida:**",
        decision[
            "message"
        ]
    )

    st.caption(
        "A recomendação combina o perfil comportamental "
        "com uma regra de decisão comercial."
    )
