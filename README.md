# 📊 E-Commerce Insights: RFM & Churn Analytics

## 📦 Contexto dos Dados (O Domínio do Negócio)
Os dados brutos utilizados neste projeto são originários de registros transacionais de comércio (dataset *"E-Commerce Insights: RFM & Churn Analysis"*). O modelo simula um cenário real de inteligência comercial para operações B2B e varejo de recorrência — como a dinâmica de fornecimento em atacado de bases de açaí e sorvetes para revendedores.

Através da metodologia **RFM (Recência, Frequência e Valor Monetário)**, o modelo isola o comportamento de compra e estabelece a regra de evasão (Churn > 90 dias sem novas compras). Se um revendedor passa 90 dias sem fazer um novo pedido, ele não parou de vender; ele provavelmente passou a comprar da concorrência. O objetivo deste projeto é transformar um mar de transações brutas em um radar de detecção tático para a equipe comercial agir preventivamente.

## 📈 O Dashboard Executivo (Power BI)

<img width="599" height="337" alt="dashboard_churn" src="https://github.com/user-attachments/assets/91ae2ee6-cf05-46f8-89fb-dd8cb651b876" />


O painel foi desenhado (Dark Mode) com foco absoluto na usabilidade e tomada de decisão rápida:
* **Impacto da Recompra:** Prova visual de que a maior taxa de evasão ocorre exatamente após a primeira compra, exigindo ações de retenção imediatas.
* **Mapa de Risco:** Gráfico de dispersão cruzando o comportamento com o faturamento, destacando outliers de alto valor.
* **Plano de Ação Tático:** Tabela gerada automaticamente entregando aos vendedores a lista priorizada de clientes de alto faturamento que acabaram de entrar na zona de risco.

## 🛠️ Arquitetura e Engenharia de Dados: ETL no Databricks
O processamento massivo dos dados brutos foi realizado na nuvem utilizando **Databricks**, combinando **PySpark** e **PySQL**. O grande desafio da engenharia de dados foi alterar a granularidade da base: achatar mais de 40 mil linhas de pedidos individuais para criar um perfil único por cliente (4.372 clientes consolidados).

Esta etapa permitiu a adoção da arquitetura **One Big Table (OBT)**. Como todo o trabalho pesado de cálculo de recência, agrupamento e classificação de Churn foi executado previamente no cluster do Apache Spark, o modelo de dados final no Power BI ficou extremamente leve e veloz, dedicado 100% à renderização visual.

<img width="1317" height="591" alt="projeto_ETL_churn_databricks_final" src="https://github.com/user-attachments/assets/e5b09bb0-677b-4c7e-9d1c-eba3de1503d2" />


### Pipeline de Transformação (Scripts)

**1. Extração e Leitura (PySpark)**
```python
caminho_csv = "/Volumes/dados-brutos/default/bucket-dados-brutos-churn/data.csv"

# Lê o CSV. O inferSchema tenta identificar automaticamente números, textos, etc.
df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load(caminho_csv)

# Cria a "Tabela Temporária"
df.createOrReplaceTempView("tabela_ecommerce")

print("Tabela carregada com sucesso!")

**2. Modelagem RFM (PySQL)**

%sql
CREATE OR REPLACE TEMP VIEW clientes_rfm AS
WITH base AS (
    SELECT 
        CustomerID,
        -- Conversão de data e hora
        MAX(to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')) as UltimaCompra,
        COUNT(DISTINCT InvoiceNo) as Frequencia,
        SUM(Quantity * UnitPrice) as ValorMonetario
    FROM tabela_ecommerce
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID
)
SELECT 
    *,
    datediff((SELECT MAX(UltimaCompra) FROM base), UltimaCompra) as Recencia
FROM base;

**3. Classificação de Churn e Tabela Final (OBT)**

%sql
CREATE OR REPLACE TEMP VIEW dataset_final AS
SELECT 
    *,
    CASE WHEN Recencia > 90 THEN 'Churn (Perdido)' ELSE 'Ativo' END as Status_Cliente
FROM clientes_rfm;

**4. Verificação**

# Consulta a tabela final e exibe na tela
df_final = spark.sql("SELECT * FROM dataset_final")
display(df_final)
