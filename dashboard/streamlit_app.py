import os
import sys

import pandas as pd
import streamlit as st
from pyathena import connect

# ============================================================
# AJUSTE DE PATH PARA IMPORTAR A MATRIZ DE DECISAO
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
# CAMINHOS
# ============================================================

METRICS_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "model_metrics.csv"
)

FEATURE_IMPORTANCE_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "feature_importance.csv"
)

SCORES_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "output",
    "conversion_scores_sample.csv"
)

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
# MAPEAMENTO DOS PERFIS
# ============================================================

PROFILE_MAP = {
    0: "Navegador Indeciso",
    1: "Comprador de Alta Intencao",
    2: "Cacador de Descontos"
}


# ============================================================
# CARGA DOS DADOS
# ============================================================

@st.cache_data
def load_csv(path):

    if os.path.exists(path):
        return pd.read_csv(path)

    return pd.DataFrame()


metrics_df = load_csv(
    METRICS_PATH
)

feature_importance_df = load_csv(
    FEATURE_IMPORTANCE_PATH
)

scores_df = load_csv(
    SCORES_PATH
)

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

st.title(
    "🛒 E-commerce Cart Recovery"
)

st.caption(
    "Plataforma de dados para análise de abandono de carrinho, "
    "propensão à conversão e recomendação de ações comerciais."
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title(
    "Navegação"
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


# ============================================================
# VISAO GERAL
# ============================================================

if page == "Visão Geral":

    st.header(
        "📊 Visão Geral do Projeto"
    )

    try:

        overview_query = f"""
        SELECT
            COUNT(*) AS cart_sessions,
            SUM(CASE WHEN is_abandoned = 1 THEN 1 ELSE 0 END) AS abandoned_sessions,
            SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS converted_sessions
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

        col1, col2, col3, col4 = st.columns(
            4
        )

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

        abandonment_rate = (
            abandoned_sessions
            /
            cart_sessions
            *
            100
        )

        conversion_rate = (
            converted_sessions
            /
            cart_sessions
            *
            100
        )

        col1, col2 = st.columns(
            2
        )

        col1.metric(
            "Taxa de abandono",
            f"{abandonment_rate:.2f}%"
        )

        col2.metric(
            "Taxa de conversão",
            f"{conversion_rate:.2f}%"
        )

        st.divider()

        st.subheader(
            "Funil de eventos"
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

        st.info(
            "Esta visão consulta diretamente a camada Gold "
            "no S3 por meio do Amazon Athena e do AWS Glue Data Catalog."
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
        "O modelo supervisionado utiliza Random Forest para "
        "estimar a probabilidade de uma sessão com carrinho "
        "apresentar comportamento semelhante a sessões que "
        "historicamente terminaram em compra."
    )

    if not metrics_df.empty:

        metrics = metrics_df.iloc[
            0
        ]

        col1, col2, col3, col4, col5 = (
            st.columns(
                5
            )
        )

        col1.metric(
            "Accuracy",
            f"{metrics['accuracy']:.2%}"
        )

        col2.metric(
            "Precision",
            f"{metrics['precision']:.2%}"
        )

        col3.metric(
            "Recall",
            f"{metrics['recall']:.2%}"
        )

        col4.metric(
            "F1 Score",
            f"{metrics['f1_score']:.2%}"
        )

        col5.metric(
            "ROC-AUC",
            f"{metrics['roc_auc']:.3f}"
        )

    else:

        st.warning(
            "Arquivo model_metrics.csv não encontrado."
        )

    st.divider()

    st.subheader(
        "Distribuição do Score de Conversão"
    )

    if not scores_df.empty:

        st.bar_chart(
            scores_df[
                "score_band"
            ]
            .value_counts()
            .rename_axis(
                "Faixa"
            )
            .to_frame(
                "Quantidade"
            )
        )

        st.write(
            "Exemplo de sessões avaliadas:"
        )

        cols = [
            "actual_converted",
            "conversion_score",
            "score_band"
        ]

        st.dataframe(
            scores_df[
                cols
            ].head(
                50
            ),
            use_container_width=True
        )

    else:

        st.warning(
            "Arquivo conversion_scores_sample.csv "
            "não encontrado."
        )

    st.divider()

    st.subheader(
        "Importância das Features"
    )

    if not feature_importance_df.empty:

        importance_chart = (
            feature_importance_df
            .sort_values(
                "importance",
                ascending=False
            )
            .set_index(
                "feature"
            )
        )

        st.bar_chart(
            importance_chart
        )

        st.dataframe(
            feature_importance_df,
            use_container_width=True
        )

    else:

        st.warning(
            "Arquivo feature_importance.csv "
            "não encontrado."
        )

    st.caption(
        "ROC-AUC superior a 0,50 indica capacidade de "
        "discriminação acima de uma classificação aleatória. "
        "O resultado atual demonstra sinal preditivo moderado "
        "e pode ser evoluído com histórico adicional e tuning."
    )


# ============================================================
# PERFIS
# ============================================================

elif page == "Perfis de Clientes":

    st.header(
        "👥 Segmentação Comportamental"
    )

    st.write(
        "O K-Means agrupa as sessões em três grupos com base "
        "nas características de navegação e do carrinho. "
        "O target de conversão não participa do clustering."
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
            use_container_width=True
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
        "Interpretação dos 3 Perfis"
    )

    st.markdown(
        """
**🎯 Comprador de Alta Intenção**  
Poucas visualizações antes do carrinho e decisão mais rápida.

**🔍 Navegador Indeciso**  
Maior volume de visualizações, mais produtos analisados e maior
tempo antes da decisão.

**🤑 Caçador de Descontos**  
Grupo com carrinho médio mais elevado, utilizado como segmento
de maior sensibilidade potencial a incentivos comerciais.
"""
    )

    if not cluster_summary_df.empty:

        st.subheader(
            "Médias das Features por Cluster"
        )

        st.dataframe(
            cluster_summary_df,
            use_container_width=True
        )

    st.caption(
        "Os nomes dos perfis são interpretações de negócio "
        "dos clusters encontrados pelo algoritmo, e não labels "
        "existentes originalmente no dataset."
    )


# ============================================================
# MATRIZ DE DECISAO
# ============================================================

elif page == "Matriz de Decisão":

    st.header(
        "🎯 Matriz Perfil × Score × Ação"
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
        "A matriz combina o perfil comportamental do cliente "
        "com o score de propensão para selecionar uma ação "
        "comercial adequada."
    )


# ============================================================
# SIMULADOR
# ============================================================

elif page == "Simulador":

    st.header(
        "🧪 Simulador de Recomendação"
    )

    st.write(
        "Selecione um perfil e defina o score de propensão "
        "para visualizar a ação recomendada."
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
        /
        100
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
        "A recomendação é baseada em uma regra de decisão "
        "comercial aplicada após os modelos de propensão "
        "e segmentação."
    )