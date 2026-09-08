\# E-commerce Cart Recovery



Projeto desenvolvido no Hands-On de Engenharia de Dados com o objetivo de construir uma plataforma de dados para análise de comportamento de usuários em um e-commerce, identificação de abandono de carrinho e recomendação de ações comerciais para recuperação de vendas.



A solução combina Engenharia de Dados, processamento distribuído, Machine Learning e visualização de dados.



\---



\## 1. Problema de negócio



O abandono de carrinho representa uma oportunidade de receita não capturada por plataformas de e-commerce.



O objetivo deste projeto é analisar o comportamento das sessões que adicionaram produtos ao carrinho e responder principalmente às seguintes perguntas:



\- Quais características estão associadas à conversão de um carrinho em compra?

\- Qual a propensão de uma sessão com carrinho apresentar comportamento semelhante às sessões que historicamente converteram?

\- Existem diferentes perfis comportamentais de clientes?

\- Qual ação comercial pode ser aplicada de acordo com o perfil e o score de propensão?



A solução foi estruturada para transformar os eventos de navegação em informações que possam apoiar uma estratégia de recuperação de carrinhos.



\---



\## 2. Dataset



Foi utilizado um dataset público de eventos de e-commerce referente a novembro de 2019.



O arquivo original possui aproximadamente 8,4 GB e contém cerca de 67,5 milhões de eventos.



Principais tipos de eventos:



\- `view`

\- `cart`

\- `purchase`



Principais campos utilizados:



\- `event\_time`

\- `event\_type`

\- `product\_id`

\- `category\_id`

\- `category\_code`

\- `brand`

\- `price`

\- `user\_id`

\- `user\_session`



Após o processamento da camada Silver, foram considerados:



\*\*67.401.460 eventos válidos.\*\*



\---



\## 3. Arquitetura da solução



A arquitetura utiliza conceitos de Data Lake, arquitetura Medallion e processamento distribuído.



Fluxo principal:



```text

Dataset de E-commerce

&#x20;       |

&#x20;       v

Ingestão Batch - Python / boto3

&#x20;       |

&#x20;       v

Amazon S3

&#x20;       |

&#x20;       +-------------------+

&#x20;       |      BRONZE       |

&#x20;       |   Dados brutos    |

&#x20;       +-------------------+

&#x20;                |

&#x20;                v

&#x20;             PySpark

&#x20;                |

&#x20;       +-------------------+

&#x20;       |      SILVER       |

&#x20;       | Dados tratados    |

&#x20;       +-------------------+

&#x20;                |

&#x20;                v

&#x20;             PySpark

&#x20;                |

&#x20;       +-------------------+

&#x20;       |       GOLD        |

&#x20;       | Features / Métricas|

&#x20;       +-------------------+

&#x20;                |

&#x20;                +----------------------+

&#x20;                |                      |

&#x20;                v                      v

&#x20;      Random Forest                 K-Means

&#x20;      Propensão à conversão         Perfis

&#x20;                |                      |

&#x20;                +----------+-----------+

&#x20;                           |

&#x20;                           v

&#x20;                   Matriz de decisão

&#x20;                           |

&#x20;                           v

&#x20;                 Recomendação comercial

&#x20;                           |

&#x20;                           v

&#x20;                        Streamlit

```



Também fazem parte da arquitetura do projeto:



\- Docker

\- Apache Kafka

\- PostgreSQL

\- Git/GitHub

\- GitHub Actions

\- Terraform



\---



\## 4. Camadas de dados



\### Bronze



A camada Bronze mantém os dados originais do dataset.



Localização:



```text

s3://ecommerce-data-platform-mack-lab/bronze/

```



Essa camada preserva o dado bruto para permitir reprocessamento e rastreabilidade.



\### Silver



Na camada Silver são realizadas operações de limpeza, padronização e preparação dos eventos.



Localização:



```text

s3://ecommerce-data-platform-mack-lab/silver/

```



Após o processamento:



\*\*67.401.460 eventos\*\* permaneceram disponíveis para análise.



\### Gold



A camada Gold transforma os eventos em informações orientadas ao problema de negócio.



Foram produzidos conjuntos como:



```text

gold/session\_features/

gold/ml\_features/

gold/funnel\_metrics/

```



A principal estrutura utilizada pelos modelos possui uma linha por sessão de carrinho.



\---



\## 5. Construção das features



As features são construídas somente com informações disponíveis até o momento do primeiro evento de carrinho quando necessário, reduzindo o risco de data leakage.



Entre as variáveis utilizadas estão:



\- quantidade de visualizações antes do carrinho;

\- quantidade de produtos visualizados;

\- quantidade de categorias visualizadas;

\- quantidade de marcas visualizadas;

\- preço médio dos produtos visualizados;

\- amplitude de preços observados;

\- valor do carrinho;

\- quantidade de itens no carrinho;

\- tempo até o primeiro carrinho;

\- relação entre visualizações e itens adicionados;

\- horário da sessão;

\- indicador de período noturno.



O evento de compra posterior é utilizado como resultado observado e não como variável preditora.



\---



\## 6. Definição do target



As sessões que apresentaram evento de carrinho foram divididas entre abandono e conversão.



Para a modelagem de propensão foi utilizado:



```text

converted = 1 -> sessão terminou em compra

converted = 0 -> sessão não apresentou compra

```



O conceito é equivalente à interpretação inversa do indicador de abandono:



```text

is\_abandoned = 1 -> carrinho abandonado

is\_abandoned = 0 -> carrinho convertido

```



No conjunto processado foram identificadas:



| Situação | Sessões |

|---|---:|

| Carrinhos abandonados | 1.094.983 |

| Sessões convertidas | 648.361 |

| Total com carrinho | 1.743.354 |



Taxa de abandono:



\*\*62,81%\*\*



Taxa de conversão:



\*\*37,19%\*\*



\---



\## 7. Modelo de propensão à conversão



Foi desenvolvido um modelo supervisionado utilizando \*\*Random Forest\*\*.



O modelo aprende os padrões observados nas sessões que historicamente converteram e gera uma probabilidade entre 0 e 1.



Exemplo:



```text

0.20 -> 20% de propensão

0.55 -> 55% de propensão

0.85 -> 85% de propensão

```



Para utilização na estratégia comercial, o score foi dividido inicialmente em três faixas:



```text

0%  - 25%  -> Baixa Propensão

25% - 75%  -> Moderada Propensão

>75%       -> Alta Propensão

```



Os limites fazem parte da regra comercial do protótipo e podem ser recalibrados em trabalhos futuros.



\### Resultados do modelo



O modelo apresentou:



| Métrica | Resultado |

|---|---:|

| Accuracy | 58,09% |

| Precision | 45,38% |

| Recall | 59,43% |

| F1 Score | 51,46% |

| ROC-AUC | 0,632 |



O resultado indica capacidade discriminativa moderada.



O objetivo desta versão é demonstrar o funcionamento completo do pipeline de Machine Learning integrado à plataforma de dados. Evoluções futuras podem incluir novas features, otimização de hiperparâmetros, calibração de probabilidades e comparação com outros algoritmos.



\---



\## 8. Segmentação comportamental



Além do modelo supervisionado, foi utilizado \*\*K-Means\*\* para identificar padrões comportamentais sem utilizar o target de conversão durante a formação dos clusters.



Foram definidos três clusters e posteriormente interpretados como:



\### Comprador de Alta Intenção



Usuários que apresentam comportamento mais direto em direção ao carrinho.



\### Navegador Indeciso



Usuários com maior comportamento de exploração e menor taxa observada de conversão.



\### Caçador de Descontos



Usuários com características associadas a maior comparação e sensibilidade a incentivos comerciais.



O clustering apresentou:



```text

Silhouette Score: 0.3977

```



Na amostra utilizada para análise dos clusters:



| Perfil | Sessões | Conversão | Abandono |

|---|---:|---:|---:|

| Navegador Indeciso | 3.413 | 31,44% | 68,56% |

| Comprador de Alta Intenção | 39.436 | 37,70% | 62,30% |

| Caçador de Descontos | 7.151 | 38,47% | 61,53% |



O target foi utilizado apenas posteriormente para interpretar o comportamento de cada cluster.



\---



\## 9. Matriz de decisão



O resultado do modelo de propensão é combinado com o perfil comportamental.



A combinação:



```text

Perfil + Score de Propensão

```



determina a ação comercial sugerida.



Exemplos:



| Perfil | Propensão | Ação |

|---|---|---|

| Comprador de Alta Intenção | Baixa | Notificação simples |

| Comprador de Alta Intenção | Moderada | Notificação de escassez |

| Comprador de Alta Intenção | Alta | Frete grátis |

| Navegador Indeciso | Baixa | Prova social |

| Navegador Indeciso | Moderada | Frete grátis |

| Navegador Indeciso | Alta | Frete grátis + 5% OFF |

| Caçador de Descontos | Baixa | Moedas / pontos |

| Caçador de Descontos | Moderada | Cupom 5% OFF ou frete |

| Caçador de Descontos | Alta | Frete grátis + 10% a 15% OFF |



A matriz representa uma regra de decisão comercial criada para demonstrar como os resultados analíticos podem ser transformados em uma ação de negócio.



\---



\## 10. Dashboard



Foi desenvolvido um dashboard em \*\*Streamlit\*\* para apresentar os principais resultados do projeto.



O dashboard contém cinco áreas:



\### Visão Geral



Apresenta:



\- eventos processados;

\- sessões com carrinho;

\- carrinhos abandonados;

\- sessões convertidas;

\- taxa de abandono;

\- taxa de conversão.



\### Modelo de Propensão



Apresenta:



\- Accuracy;

\- Precision;

\- Recall;

\- F1 Score;

\- ROC-AUC;

\- distribuição dos scores;

\- resultados das sessões avaliadas.



\### Perfis de Clientes



Apresenta:



\- Silhouette Score;

\- distribuição dos clusters;

\- conversão por perfil;

\- abandono por perfil.



\### Matriz de Decisão



Apresenta as combinações entre perfil, score e ação comercial.



\### Simulador



Permite selecionar um perfil e um score de propensão para visualizar a ação recomendada.



Exemplo:



```text

Perfil: Comprador de Alta Intenção

Score: 85%

Faixa: Alta Propensão



Ação:

Frete Grátis

```



\---



\## 11. Testes e validação funcional



Foram implementados testes automatizados utilizando \*\*pytest\*\*.



Os testes validam:



\- classificação das faixas de score;

\- limites entre as faixas;

\- nove combinações da matriz de decisão;

\- mapeamento dos três clusters;

\- scores inválidos;

\- perfis inválidos;

\- clusters inválidos.



Resultado:



```text

19 passed

```



Além dos testes automatizados, foram realizados testes funcionais no dashboard para validar a apresentação dos resultados e o simulador de recomendação.



\---



\## 12. CI/CD



Foi configurado um pipeline de integração contínua utilizando \*\*GitHub Actions\*\*.



O workflow é executado automaticamente em:



```text

push -> main

pull\_request -> main

```



Etapas executadas:



```text

Checkout

&#x20;  |

&#x20;  v

Configuração do Python

&#x20;  |

&#x20;  v

Instalação das dependências

&#x20;  |

&#x20;  v

Validação de sintaxe

&#x20;  |

&#x20;  v

Execução dos testes automatizados

```



O pipeline foi executado e validado com sucesso no GitHub Actions.



Arquivo:



```text

.github/workflows/ci.yml

```



\---



\## 13. Tecnologias utilizadas



\### Engenharia de Dados



\- Python

\- PySpark

\- Apache Spark

\- Apache Kafka

\- PostgreSQL



\### Cloud



\- AWS

\- Amazon S3

\- boto3



\### Machine Learning



\- pandas

\- scikit-learn

\- Random Forest

\- K-Means

\- joblib



\### Visualização



\- Streamlit



\### Infraestrutura e DevOps



\- Docker

\- Docker Compose

\- Terraform

\- Git

\- GitHub

\- GitHub Actions



\---



\## 14. Estrutura do projeto



```text

ecommerce-data-platform/

|

|-- app/

|   |-- batch\_ingestion.py

|   |-- gold\_processor.py

|   |-- ...

|

|-- src/

|   |-- train\_conversion\_propensity.py

|   |-- train\_customer\_profiles.py

|   |-- decision\_matrix.py

|   |-- ...

|

|-- dashboard/

|   |-- streamlit\_app.py

|

|-- tests/

|   |-- test\_decision\_matrix.py

|

|-- terraform/

|

|-- .github/

|   |-- workflows/

|       |-- ci.yml

|

|-- docker-compose.yml

|-- requirements.txt

|-- README.md

```



\---



\## 15. Execução do dashboard



Com o ambiente Python configurado:



```bash

python -m streamlit run dashboard/streamlit\_app.py

```



O dashboard fica disponível localmente em:



```text

localhost:8501

```



\---



\## 16. Principais resultados



O projeto permitiu construir um fluxo completo desde a ingestão até a geração de uma recomendação de negócio.



Foram processados mais de \*\*67 milhões de eventos\*\*, resultando em aproximadamente \*\*1,74 milhão de sessões com carrinho\*\*.



A taxa observada de abandono foi de \*\*62,81%\*\*, demonstrando o potencial de atuação sobre sessões que não finalizaram a compra.



A modelagem permitiu criar um score de propensão à conversão e a segmentação comportamental identificou três grupos distintos de sessões.



A combinação dos dois resultados permitiu construir uma matriz de decisão para direcionar diferentes ações comerciais.



\---



\## 17. Limitações e próximos passos



O projeto representa um MVP funcional e possui oportunidades de evolução.



Entre os próximos passos estão:



\- testar novos algoritmos de classificação;

\- melhorar a engenharia de features;

\- realizar tuning de hiperparâmetros;

\- calibrar o score de probabilidade;

\- revisar os thresholds das faixas de propensão;

\- avaliar uplift/incrementalidade das ações comerciais;

\- realizar testes A/B para medir a efetividade dos incentivos;

\- evoluir o processamento de eventos Kafka;

\- estruturar monitoramento de Data Quality;

\- ampliar a automação da infraestrutura com Terraform;

\- implementar monitoramento de drift do modelo;

\- acompanhar métricas de negócio como conversão incremental e receita recuperada.



\---



\## 18. Conclusão



A solução demonstra a construção de uma plataforma de dados de ponta a ponta aplicada a um problema real de e-commerce.



O projeto integra processamento de dados em larga escala, arquitetura Medallion, armazenamento em Data Lake, processamento distribuído, Machine Learning, segmentação comportamental, regras de decisão, testes automatizados, CI/CD e visualização dos resultados.



Mais do que identificar carrinhos abandonados, a proposta é utilizar os dados para estimar a propensão à conversão e direcionar ações comerciais diferentes de acordo com o comportamento de cada grupo de clientes.

