# 📦 Data Pipeline

Projeto robusto de pipelines de dados para tratamento, transformação e análise de dados logísticos. Estruturado de forma modular, com foco em reutilização de código, logging centralizado e escalabilidade, o **Data Pipeline** viabiliza a ingestão, limpeza, processamento e integração de dados operacionais, com suporte a múltiplos fluxos simultâneos.

## 📁 Estrutura do Projeto

data_pipeline/
│
├── config/
│   └── settings.py                 # Configurações fixas e variáveis globais
│
├── data/
│   ├── raw/                        # CSVs puros de entrada
│   │   ├── data_olpn.csv
│   │   └── data_picking.csv
│   │   └── data_packing.csv
│   │   └── data_cancel.csv
│   │   └── data_load.csv
│   │   └── data_putaway.csv
│   │   └── data_jornada.csv
│   │
│   ├── processed/                  # CSVs limpos e transformados
│   │   ├── data_olpn.csv
│   │   └── data_picking.csv
│   │   └── data_packing.csv
│   │   └── data_cancel.csv
│   │   └── data_load.csv
│   │   └── data_putaway.csv
│   │   └── data_jornada.csv
│   │
│   └── parquet/                    # Arquivos Parquet finais
│       ├── data_olpn.parquet
│       └── data_picking.parquet
│       └── data_packing.parquet
│       └── data_cancel.parquet
│       └── data_load.parquet
│       └── data_putaway.parquet
│       └── data_jornada.parquet
│
├── integrated_pipelines/
│   ├── __init__.py
│   ├── bottleneck_box.py           # Pipeline específico para análise de bottleneck box
│   ├── bottleneck_salao.py         # Pipeline específico para análise de bottleneck salão
│   ├── jornada_pipeline.py         # Pipeline específico para análise de jornada
│   └── time_lead_olpn.py
│
├── logs/
│   └── pipeline.log                # Saída de logs centralizada
│
├── pipeline/
│   ├── __init__.py
│   ├── base_pipeline.py            # Classe base para pipelines
│   ├── olpn_pipeline.py            # Pipeline específico do OLPn
│   ├── picking_pipeline.py         # Pipeline específico do Picking
│   ├── cancel_pipeline.py          # Pipeline específico do Cancel
│   ├── jornada_pipeline.py         # Pipeline específico da Jornada
│   ├── load_pipeline.py            # Pipeline específico do Load
│   ├── packing_pipeline.py         # Pipeline específico do Packing
│   ├── putaway_pipeline.py         # Pipeline específico do Putaway
│
├── utils/
│   ├── __init__.py
│   ├── io.py                       # Leitura/escrita de arquivos
│   ├── transformation.py           # Funções de transformação
│   ├── classification.py           # Lógicas de classificação
│   └── logging_utils.py            # Configuração de logs
│                  
├── process_pipeline.py             # Script principal que roda todos os pipelines
├── requirements.txt
└── README.md                       # Explicações do projeto

---

## 🎯 Objetivo

Automatizar o tratamento e análise de dados operacionais logísticos, garantindo consistência, rastreabilidade e performance no fluxo de ponta a ponta, desde a ingestão de arquivos brutos até a geração de arquivos Parquet prontos para análise em ferramentas de BI.

---

## Visão Geral

### Módulo `settings.py`

Este módulo centraliza as configurações globais e específicas de cada pipeline do projeto **Data Pipeline**, incluindo caminhos de dados, parâmetros de leitura e escrita, definições de tipos de dados, regras de transformação e mapeamentos de negócio. Atua como fonte única de verdade para os pipelines, garantindo coerência e facilidade de manutenção.

---

### Módulo `bottleneck_box.py`

O módulo `bottleneck_box.py` implementa o pipeline dedicado à análise do gargalo operacional denominado "bottleneck box". Utilizando leitura eficiente de arquivos Parquet particionados, o pipeline integra dados dos estágios de `load` e `putaway`, aplicando regras de tratamento de dados configuradas centralmente para garantir padronização e qualidade.

A lógica central consiste em combinar os datasets via chave única (`olpn`), realizar conversões temporais para cálculo do tempo entre eventos críticos, e exportar os resultados segmentados por mês para facilitar análises temporais e geração de relatórios. Todo processo é monitorado via logging robusto, assegurando rastreabilidade e facilidade de diagnóstico em ambiente produtivo.

---

### Módulo `bottleneck_salao.py`

O módulo `bottleneck_salao.py` implementa o pipeline voltado para análise do gargalo operacional denominado "bottleneck salão". O pipeline realiza a leitura eficiente de arquivos Parquet particionados referentes às etapas de `packed` e `putaway`, consolidando os dados com base na chave única `olpn`.

Através da união dos datasets e conversão de colunas temporais, calcula o intervalo entre eventos críticos para identificar e mensurar o gargalo. Os resultados são exportados em arquivos segmentados mensalmente, facilitando análises temporais e o monitoramento contínuo. Todo o processo é suportado por um sistema de logging estruturado, garantindo rastreabilidade e confiabilidade operacional.

---

### Módulo `jornada_pipeline.py`

O módulo `jornada_pipeline.py` implementa o pipeline focado no processamento de dados de jornada de trabalho. Ele realiza o pré-processamento estruturado de arquivos CSV, aplicando limpeza, padronização e enriquecimento dos dados conforme configurações específicas do pipeline.

Além disso, converte arquivos CSV processados para o formato Parquet, otimizando armazenamento e performance para análises subsequentes. O pipeline suporta segmentação dos dados por mês para facilitar a gestão temporal e conta com logging detalhado para garantir monitoramento e rápida identificação de falhas durante a execução.

---

### Módulo `time_lead_olpn.py`

O módulo `time_lead_olpn.py` implementa o pipeline responsável pela análise do lead time do processo OLPN. Ele realiza a leitura de múltiplos arquivos Parquet, aplicando pré-processamento com validação, limpeza, tratamento de tipos e cálculo do tempo absoluto entre eventos-chave (`data_hora_load` e `data_pedido`).

Os dados são segmentados e exportados por mês, garantindo granularidade temporal para análises operacionais e de performance. A solução incorpora logging avançado para rastreamento detalhado, assegurando transparência e controle durante toda a execução do pipeline.

---

### Módulo `base_pipeline.py`

O módulo `base_pipeline.py` define a classe abstrata `BasePipeline`, que serve como estrutura base para a implementação dos pipelines específicos do projeto. Ela encapsula a lógica central de leitura de arquivos CSV, pré-processamento, validação e exportação dos dados segmentados por mês, tanto em CSV quanto em Parquet.

A classe promove padronização e reutilização, exigindo que subclasses implementem o método `preprocess` para aplicar regras específicas de transformação. O uso de logging robusto assegura controle operacional e fácil diagnóstico durante a execução dos pipelines.

---

### Módulo `cancel_pipeline.py`

Este módulo implementa o `CancelPipeline`, uma extensão da classe base `BasePipeline`, dedicada ao processamento dos dados de cancelamento.

A pipeline executa etapas estruturadas de limpeza e transformação, incluindo:
- Remoção e renomeação de colunas conforme configuração específica.
- Conversão de tipos de dados e normalização de datas.
- Classificação dos setores com função customizada `cancel_classify_setores`.
- Tratamento avançado da coluna `motivo_cancelamento`, aplicando normalização textual (remoção de acentos, padronização de caixa) e categorização via a função `normalizar_motivo`.
- Geração de colunas auxiliares para análise temporal (`hora`, `data_criterio`, `mes_ano`) baseadas na data do cancelamento.

Essa arquitetura promove governança robusta, assegurando qualidade e consistência dos dados para análises avançadas e dashboards corporativos.

---

### Módulo `load_pipeline.py`

Este módulo define o `LoadPipeline`, uma especialização da classe base `BasePipeline` focada no tratamento dos dados de carregamento (`load`).

O processo de pré-processamento contempla:

- Limpeza estruturada removendo e renomeando colunas conforme configuração centralizada.
- Conversão rigorosa de tipos e tratamento de colunas temporais.
- Extração da data base a partir da coluna `data_hora_load`.
- Cálculo do tempo total de atividade diária por box (`duracao_segundos_box_dia`), agregando as mínimas e máximas marcações temporais por combinação de box e data.
- Geração de colunas temporais padronizadas para análises granulares (`hora`, `data_criterio`, `mes_ano`).
- Classificação dos setores com função dedicada `classify_setores`.
- Limpeza final removendo colunas auxiliares resultantes de merges intermediários.

A pipeline reforça a governança dos dados operacionais, garantindo a consistência e enriquecimento temporal para análises avançadas e monitoramento estratégico.  

---

### Módulo `olpn_pipeline.py`

Este módulo implementa a classe `OlpnPipeline`, derivada de `BasePipeline`, destinada ao pré-processamento dos dados OPLN (Order Picking Location Number).

Pontos-chave do processamento:

- Remoção e renomeação de colunas conforme configuração centralizada.
- Conversão robusta de tipos, incluindo tratamento especial para a coluna `volume` com conversão numérica e substituição de vírgulas.
- Extração detalhada do campo `local_de_picking` em três componentes (`rua`, `endereco` e `nivel`), facilitando análises granulares de localização.
- Classificação de setores por meio da função `classify_setores`.
- Construção da coluna `localizacao` com lógica condicional baseada em valores específicos, distinguindo entre área "P.A.R" e "Salao".
- Definição de partições temporais (`mes_ano`, `data_criterio`, `hora`) fundamentadas na coluna `data_hora_ultimo_update_olpn`, com fallback controlado para ausência desse dado.

Essa pipeline é essencial para estruturar, enriquecer e segmentar os dados logísticos, habilitando análises espaciais e temporais avançadas dentro do contexto operacional.

---

### Módulo `packing_pipeline.py`

Este módulo implementa a classe `PackingPipeline`, que herda de `BasePipeline` e é responsável pelo processamento dos dados de packing, com integração da referência OLPN para enriquecer o dataset.

Destaques do processamento:

- Carregamento da base OLPN via função especializada `read_parquet_with_tote` para integração de dados.
- Aplicação de transformações configuradas, incluindo remoção e renomeação de colunas, ajuste de tipos e normalização textual da coluna `desc_setor_item`.
- Merge entre os dados de packing e a referência OLPN com chave `olpn`, agregando informações cruciais.
- Cálculo do tempo de atividade diário por `box` e por `tote` baseado em timestamps, gerando métricas de duração em segundos.
- Ajuste para garantir que durações de tote menores que 60 segundos sejam normalizadas para 60 segundos, assegurando consistência analítica.
- Extração e formatação de partições temporais (`hora`, `data_criterio`, `mes_ano`) a partir da coluna `data_hora_packed`.
- Classificação de setores para segmentação operacional.
- Limpeza final dos dados para eliminar colunas intermediárias irrelevantes.

Essa pipeline garante uma visão integrada e temporalmente segmentada do processo de packing, essencial para análises de eficiência operacional e controle logístico.

---

### Módulo `picking_pipeline.py`

Este módulo define a classe `PickingPipeline`, especializada no processamento dos dados de picking, derivando da classe base `BasePipeline`.

Principais funcionalidades e transformações:

- Aplicação de limpeza e padronização via remoção e renomeação de colunas conforme configuração.
- Conversão de tipos de dados e parsing de datas para garantir integridade e coerência temporal.
- Cálculo do tempo de duração da tarefa (`duracao_tarefa_segundos`) a partir das colunas de início e fim da tarefa.
- Desmembramento da coluna `local_de_picking` em subcomponentes (`rua`, `endereco`, `nivel`) para granularidade espacial.
- Preenchimento de valores ausentes nos campos de localização para evitar inconsistências.
- Classificação da localização em categorias operacionais ("P.A.R" ou "Salao") baseada em regras definidas para ruas e endereços.
- Atribuição de setores via função de classificação específica, promovendo segmentação analítica.
- Geração de colunas temporais de particionamento (`mes_ano`, `data_criterio`, `hora`) baseadas no timestamp de término da tarefa.
- Logging de alertas quando informações essenciais para particionamento temporal estão ausentes.

Esta pipeline promove a análise detalhada do desempenho e eficiência das operações de picking, com foco em segmentação espacial e temporal para suportar decisões estratégicas e táticas.

---

### Módulo `putaway_pipeline.py`

Este módulo implementa a classe `PutawayPipeline`, especializada no processamento dos dados de putaway, estendendo a estrutura do `BasePipeline`.

Pontos-chave da pipeline:

- Aplicação de limpeza, remoção e renomeação de colunas conforme definido nas configurações específicas.
- Conversão robusta de tipos, incluindo extração numérica da coluna `box` para padronização.
- Parsing e validação das colunas de data/hora para garantir coerência temporal.
- Cálculo do tempo total por box e dia, a partir das timestamps mínimas e máximas, refletindo a duração operacional diária.
- Integração dos cálculos de tempo com o DataFrame principal via merge, para análise consolidada.
- Construção de colunas para particionamento temporal (`hora`, `data_criterio`, `mes_ano`) com base no timestamp de putaway.
- Classificação dos dados em setores operacionais por meio da função `classify_setores`.
- Limpeza final removendo colunas temporárias ou redundantes para otimizar o dataset.
- Logging para monitoramento do fluxo e alertas sobre ausência de dados críticos.

Esta pipeline é essencial para mensurar e analisar a eficiência das operações de putaway, suportando decisões que impactam diretamente a produtividade e a gestão do fluxo logístico.

---

### Módulo `classification.py`

Este módulo contém funções essenciais para classificação setorial dos dados operacionais, fundamental para segmentação analítica e tomada de decisão orientada por contexto.

### Função `classify_setores`

- Recebe um DataFrame com informações de pedidos e boxes.
- Aplica múltiplas condições lógicas compostas por combinações de `tipo_de_pedido` e faixa numérica de `box`.
- Cada condição associa um rótulo setorial específico (ex: "Fracionado Pesados", "EAD - Abastecimento de Lojas", "Ribeirao Preto", etc.).
- Converte a coluna `box` para inteiro, tratando valores inválidos.
- Retorna uma série com a classificação setorial baseada nas condições.
- Valor default para registros que não atendem nenhuma condição é "Outras Saidas".

### Função `cancel_classify_setores`

- Similar à função anterior, mas focada em classificação para dados de cancelamento.
- Avalia apenas o campo `tipo_de_pedido` para atribuir os setores.
- Também retorna uma série categorizada, com default "Outras saidas".

### Módulo `io.py`

Este módulo é responsável pela leitura eficiente de arquivos de entrada nos pipelines, garantindo resiliência, performance e consistência com as configurações de cada fluxo de dados.

### Função: `read_all_csv`

**Objetivo:**  
Ler todos os arquivos `.csv` de um diretório específico, utilizando `chunks` para eficiência de memória.

**Parâmetros:**
- `folder: Path` → Caminho para a pasta contendo os arquivos `.csv`.
- `pipeline_key: str` → Chave de identificação do pipeline no `PIPELINE_CONFIG`.

**Funcionamento:**
- Recupera configurações específicas de encoding e separador do pipeline.
- Itera por todos os arquivos `.csv` no diretório, utilizando `pd.read_csv()` com `chunksize` (carregamento em blocos).
- Todos os chunks são concatenados ao final em um único `DataFrame`.
- Em caso de erro durante a leitura de qualquer arquivo, registra no log e prossegue com os demais.

**Retorno:**
- `pd.DataFrame` consolidado contendo os dados de todos os arquivos `.csv`.

### Função: `read_parquet_with_tote`

**Objetivo:**  
Ler múltiplos arquivos `.parquet` contendo apenas as colunas `olpn` e `tote`.

**Parâmetros:**
- `folder: Path` → Caminho da pasta com arquivos `.parquet`.

**Funcionamento:**
- Itera sobre os arquivos `.parquet` da pasta.
- Para cada arquivo, lê somente as colunas `['olpn', 'tote']`.
- Concatena os `DataFrames` e remove duplicatas com base em `olpn`.

**Retorno:**
- `pd.DataFrame` único contendo os pares `olpn`-`tote` sem duplicações.

### Observações Estratégicas

- O uso de leitura por chunks (`CHUNK_SIZE`) no `read_all_csv` é uma prática robusta para evitar estouro de memória em arquivos extensos.
- Ambos os métodos são projetados para integrar-se diretamente ao sistema de configuração modular `PIPELINE_CONFIG`, conferindo escalabilidade e adaptabilidade.
- O `read_parquet_with_tote` é um ponto central de integração entre pipelines, como visto no `packing_pipeline`, o que reforça a necessidade de consistência e limpeza nos dados de referência `olpn`.

---

### Módulo `logging_utils.py`

Este módulo é responsável pela configuração centralizada do sistema de **log corporativo** dos pipelines de dados, garantindo rastreabilidade, diagnóstico eficiente e rotação controlada de arquivos.

### Função: `setup_logging`

**Objetivo:**  
Configurar e retornar um logger padrão para todos os pipelines, com suporte a rotação de arquivos e saída simultânea no console.

**Parâmetros:**
- `level: logging.Level` → Nível de logging desejado (padrão: `logging.INFO`).

**Funcionamento:**
1. **Validação de Caminho:**
   - Garante que o diretório definido em `LOG_PATH` (via `config.settings`) exista. Caso contrário, é criado.

2. **Instanciação do Logger:**
   - Um logger nomeado como `'pipeline_logger'` é configurado com o nível especificado.
   - O logger utiliza um formato padronizado de log:  
     `YYYY-MM-DD HH:MM:SS | LEVEL | MENSAGEM`

3. **Rotação de Arquivos:**
   - Usa `RotatingFileHandler` para gravar logs em disco.
   - Tamanho máximo por arquivo: `10 MB`
   - Retenção: até 5 arquivos de histórico (`backupCount=5`)

4. **Saída Simultânea no Console:**
   - Também adiciona um `StreamHandler` para exibir logs no terminal.

5. **Evita Duplicidade de Handlers:**
   - O logger só adiciona handlers caso ainda não existam, prevenindo múltiplas saídas duplicadas em reexecuções.

**Retorno:**
- Objeto `logger` já configurado, pronto para ser utilizado com `.info()`, `.warning()`, `.error()`, etc.

### Boas Práticas Corporativas Embutidas:

- **Persistência com rotação:** evita crescimento descontrolado de arquivos de log.
- **Log estruturado e datado:** facilita leitura por humanos e integração com ferramentas de observabilidade (ex: ELK, Splunk).
- **Console + arquivo:** habilita rastreabilidade local e arquivamento histórico simultaneamente.

---

### Módulo `transformation.py`

Este módulo centraliza funções utilitárias de transformação e exportação de dados, com foco em:
- Padronização de outputs mensais
- Conversão de formatos (CSV ↔ Parquet)
- Normalização de valores categóricos com base em regras de negócio

---

### 1. `export_by_month(df, output_folder, pipeline_key)`

**Objetivo:**  
Exportar `DataFrame` particionado por mês (`mes_ano`) em arquivos `.csv`, respeitando configurações específicas de cada pipeline.

**Parâmetros:**
- `df`: `pd.DataFrame` contendo a coluna `mes_ano`
- `output_folder`: `Path` onde os arquivos serão salvos
- `pipeline_key`: chave de configuração em `PIPELINE_CONFIG`

**Características:**
- Valida a existência da configuração
- Exporta com `utf-16` e `sep='\t'` (compatibilidade com Excel)
- Nome do arquivo: `dados_MM-AAAA.csv`
- Drop da coluna `mes_ano` no arquivo final

### Módulo `process_pipeline.py`

Este script orquestra a execução completa do ecossistema de pipelines modulares do projeto, sendo o ponto de entrada principal para o processamento em lote dos dados operacionais.

---

### 📌 Objetivo Geral

Consolidar e padronizar a execução sequencial dos seguintes fluxos:

- **Pipelines baseados em `BasePipeline`**: processam arquivos CSV brutos em estruturas normalizadas e particionadas
- **Pipelines autogerenciados (integrados)**: realizam agregações, cruzamentos e cálculos compostos entre múltiplas fontes já tratadas

---

### 🔁 Fluxo de Execução

#### 1. **Inicialização**

- Configura o `logger`
- Define `start_time` para mensuração de performance
- Carrega `PIPELINE_PATHS` para leitura e escrita de arquivos

---

#### 2. **Execução dos Pipelines Base**

Executa os seguintes pipelines, herdando de `BasePipeline`:

| Pipeline    | Função                                    |
|-------------|--------------------------------------------|
| `olpn`      | Processamento de dados de OLPN            |
| `picking`   | Análise de tarefas de separação           |
| `cancel`    | Normalização e classificação de cancelamentos |
| `load`      | Identificação de carga e tempos por box   |
| `putaway`   | Armazenamento e tempo por box             |
| `packing`   | Empacotamento e tempos por tote/box       |

**Para cada pipeline:**
- Caminhos de entrada, saída `.csv` e `.parquet` são resolvidos via `PIPELINE_PATHS`
- Diretórios são criados caso não existam
- Pipeline é executado via `.run()`
- Resultado é validado (`df.empty`) e logado

---

#### 3. **Execução dos Pipelines Integrados**

Executa pipelines com estrutura independente (não usam `BasePipeline`), projetados para lógica de negócio composta:

| Pipeline               | Descrição                                                        |
|------------------------|------------------------------------------------------------------|
| `JornadaPipeline`      | Consolida jornada logística por OLPN (integra vários pipelines) |
| `TimeLeadOLPNPipeline` | Calcula tempo de ciclo entre eventos por OLPN                   |
| `BottleneckSalao`      | Identifica gargalos operacionais no "salão" logístico          |
| `BottleneckBox`        | Analisa tempo de retenção por `box` entre `load` e `putaway`   |

---

### 🧠 Boas Práticas Aplicadas

- **Modularização extensível** com uso de `BasePipeline` e pipelines autônomos
- **Orquestração robusta e tolerante a falhas**
- **Monitoramento completo via logging estruturado**
- **Particionamento por `mes_ano` e persistência eficiente em `.parquet`**

---

### ⏱️ Métricas de Performance

- O tempo total de execução é registrado e logado com `time.time()`
- Permite análise posterior de SLA do pipeline completo

---