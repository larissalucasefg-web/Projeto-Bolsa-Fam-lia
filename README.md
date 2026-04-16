# 📊 Análise de Dados - Bolsa Família 2021 com PySpark

Este projeto realiza o processamento e a análise exploratória dos dados de pagamentos do programa **Bolsa Família** referentes ao ano de 2021.

Utilizando o ecossistema **Apache Spark**, o código é capaz de manipular grandes volumes de dados (**Big Data**) de forma eficiente, realizando desde a limpeza e padronização até a geração de insights financeiros e geográficos.

---

## 📁 Estrutura do Projeto


📦 PROJETO_BOLSA_FAMILIA\
├── 📂 dados\
│ └── 📄 pagamentos.csv\
├── 📂 notebooks\
│ └── 📄 analise_exploratoria.ipynb\
├── 📂 src\
│ ├── 📂 pycache\
│ ├── 📄 analise_dados.py\
│ ├── 📄 graficos.py\
│ ├── 📄 leitura_dados.py\
│ ├── 📄 processamento.py\
│ └── 📄 tratamento_dados.py\
├── 📂 venv\
├── 📄 .gitignore\
├── 📄 README.md\
└── 📄 requirements.txt

---

## 🚀 Tecnologias Utilizadas

- **Python 3.x** → Linguagem principal para desenvolvimento da lógica  
- **PySpark (Spark SQL)** → Processamento distribuído e manipulação de grandes datasets  
- **Pandas** → Conversão de dados e suporte a visualização  
- **Matplotlib** → Geração de gráficos estatísticos  

---

## 📋 Funcionalidades do Projeto

O pipeline de dados está estruturado em quatro etapas fundamentais:

---

### 1. ⚙️ Configuração do Ambiente Big Data

O ambiente é otimizado para lidar com arquivos volumosos através da **SparkSession**, com:

- **Memória:** 8GB para Driver e Executor  
- **Shuffle:** Ajuste de partições (`spark.sql.shuffle.partitions`) para otimizar operações  

---

### 2. 🔄 ETL (Extração, Transformação e Carga)

**Leitura**
- Importação de arquivos CSV  
- Separador `;`  
- Encoding `ISO-8859-1`  

**Padronização**
- Conversão de colunas para `snake_case` via dicionário  

**Limpeza**
- Remoção de valores nulos (`dropna`)  

**Casting**
- Conversão de valores monetários (formato BR) para `Decimal(10,2)`  

---

### 3. 📊 Análise Exploratória

O projeto gera automaticamente indicadores importantes:

- **Indicadores Financeiros**
  - Total pago  
  - Média dos pagamentos  

- **Visão Regional**
  - Estados (UF) com maior volume de recursos  

- **Ranking de Beneficiários**
  - Top 10 com maiores valores acumulados  

---

### 4. 📉 Visualização de Dados

- Conversão de dados do Spark para **Pandas**
- Criação de gráfico de barras com:
  - Top 10 estados com maior volume de pagamento  

---

## 🧱 Estrutura do Código

O projeto segue boas práticas de engenharia de dados:

- **SparkSession** → Inicialização do processamento distribuído  
- **Method Chaining** → Pipeline de transformação mais legível  
- **Agregações** → Uso de `groupBy` e `agg`  
- **Visualização** → Geração de gráficos para análise  

---

## ⚙️ Como Executar

### ✅ Pré-requisitos

- Ter o **Apache Spark** instalado e configurado  
- Instalar as dependências

## 📈 Exemplo de Insight Gerado

A comparação entre a média individual de cada parcela recebida versus o valor total acumulado por CPF permite auditar os perfis dos maiores recebedores do programa e identificar outliers na distribuição regional.

## 🎯 Finalidade

Este projeto foi desenvolvido para fins de estudo em:

Engenharia de Dados
Big Data com PySpark
Análise de políticas públicas

## 👩‍💻 Autora

Larissa Lucas Tavares


