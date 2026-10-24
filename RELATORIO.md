# Relatório Executivo e Técnico: Datalake de Tracking da DadosPro

## 1. Visão Geral da Arquitetura

O Datalake de Tracking da DadosPro foi construído com arquitetura medalhão em três camadas sobre armazenamento orientado a objetos compatível com AWS S3 (`s3a://datalake-tracking/`), orquestrado via PySpark e consultado via Glue Data Catalog e AWS Athena:

- **Bronze (Armazenamento Cru / Proveniência)**:
  Ingestão append-only dos arquivos semiestruturados da landing (`JSON Lines`), aplicando schema estrito e explícito sem inferência dinâmica, particionado por evento e data (`event_name=<nome>/year=AAAA/month=MM/day=DD`) e enriquecido com metadados de auditoria e proveniência (`_arquivo_origem` e `_ingestao_ts`).
- **Silver (Higienização, Conformidade e Modelagem)**:
  Camada limpa e normalizada. Realiza deduplicação por `event_id`, validação rigorosa das 6 regras de contrato de dados (isolando rejeições em `silver/eventos_rejeitados/` com motivo explícito), extração de todos os parâmetros para colunas tipadas de primeiro nível, sessionização por inatividade de 30 minutos (descartando `session_hint`), *identity stitching* retroativo conectando `user_pseudo_id` a `user_id`, e desaninhamento da matriz de produtos para a tabela granular `silver/itens/`.
- **Gold (Métricas de Negócio, Previsão e Visualização)**:
  Conjunto de 15 tabelas analíticas otimizadas para consumo de BI, machine learning e visualização em tempo real (mapa de calor), respondendo integralmente às 4 perguntas de negócio da empresa.

---

## 2. Respostas às Quatro Perguntas de Negócio

### Pergunta 1: Funil de Vendas
> **Em que etapa a jornada de compra perde mais gente, por plano e por canal?**

- **Tabela de Sustentação**: [`gold.funil_compra`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_funil.py)
- **Evidências Numéricas**:
  - **Sessões Totais**: 8.793 sessões registradas no período.
  - **Etapa 1 - Visualização dos Planos (`view_item_list`)**: 5.258 sessões (59,80% do tráfego total chega até a tabela de preços).
  - **Etapa 2 - Seleção de Plano (`select_item`)**: 1.719 sessões.
    - **Ponto Crítico de Abandono**: A conversão de `view_item_list` para `select_item` é de apenas **32,69%**, representando uma **perda de 67,31% dos visitantes**. Este é o maior gargalo absoluto de toda a jornada comercial.
  - **Etapas Subsequentes**:
    - `select_item` → `add_to_cart`: 1.291 sessões (conversão de 75,10%).
    - `add_to_cart` → `view_cart`: 1.255 sessões (conversão de 97,21%).
    - `view_cart` → `begin_checkout`: 912 sessões (conversão de 72,67% — perda de 27,33% ao transicionar para o checkout).
    - `begin_checkout` → `add_payment_info`: 684 sessões (conversão de 75,00%).
    - `add_payment_info` → `purchase`: 573 sessões (conversão de 83,77%).
  - **Conversão de Ponta a Ponta**: 10,90% das sessões que visualizaram os planos concluíram a compra.
  - **Abertura de Detalhes (`view_item`)**: 57,30% das sessões que selecionaram um plano abriram o modal/página de detalhe antes de comprar.
- **Análise por Plano e Canal**:
  - O `PLANO-PRO` é o campeão de vendas e seleção, seguido por `PLANO-BASICO` e `PLANO-PREMIUM`.
  - O canal `google / cpc` traz o maior volume de tráfego, porém com dispersão maior na tabela de preços. Já o tráfego `email / newsletter` e `(direct) / (none)` apresenta taxas de retenção no checkout superiores a 82%.
- **Recomendação Acionável**:
  1. Redesenhar a tabela de planos da landing page, destacando visualmente o `PLANO-PRO` como "Mais Escolhido" e simplificando a matriz de benefícios para reduzir a sobrecarga cognitiva que causa o drop de 67%.
  2. Implementar depoimentos em vídeo e garantias explícitas diretamente na seção de preços para incentivar o clique em "Selecionar Plano".

---

### Pergunta 2: Engajamento
> **O vídeo de apresentação é visto até o fim? O que o visitante procura no FAQ e não encontra?**

- **Tabelas de Sustentação**: [`gold.retencao_video`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_engajamento.py) e [`gold.buscas_faq`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_engajamento.py)
- **Evidências Numéricas**:
  - **Vídeo de Apresentação (VSL)**:
    - Sessões que deram play (`video_start`): 2.932 sessões.
    - Retenção a 10%: 2.668 sessões (91,00%).
    - Retenção a 25%: 2.173 sessões (74,11%).
    - Retenção a 50%: 1.499 sessões (51,13%).
    - Retenção a 75%: 1.126 sessões (38,40%).
    - Retenção a 100% (Conclusão): 888 sessões (**30,29%**).
    - Quase 49% do público abandona antes da metade do vídeo, mas quem ultrapassa os 50% apresenta altíssima probabilidade de retenção até o fim (59,2% dos que chegam a 50% assistem até o final).
  - **Buscas no FAQ Sem Resposta (`search_results = 0`)**:
    - `estagio`: 44 buscas (100% sem resposta).
    - `presencial`: 33 buscas (100% sem resposta).
    - `bolsa`: 32 buscas (100% sem resposta).
    - `power bi`: 28 buscas (100% sem resposta).
    - `linux`: 24 buscas (100% sem resposta).
- **Recomendação Acionável**:
  1. **Otimização do VSL**: Antecipar a apresentação da proposta de valor, garantia e call-to-action nos primeiros 25% a 30% do vídeo (antes do ponto de evasão de 48,8%).
  2. **Atualização Imediata do FAQ**: Adicionar perguntas e respostas oficiais esclarecendo: se o curso oferece suporte para estágio e vagas; se o modelo é 100% online ou presencial; políticas de bolsas de estudo; presença de módulos de Power BI; e suporte a ambientes Linux.

---

### Pergunta 3: Receita e Aquisição
> **Quanto entrou de fato, descontados os reembolsos, e qual canal traz cliente que não desiste?**

- **Tabelas de Sustentação**: [`gold.receita_diaria`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_receita.py), [`gold.taxa_reembolso`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_receita.py), [`gold.adesao_order_bump`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_receita.py), [`gold.lead_para_compra`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_receita.py)
- **Evidências Numéricas**:
  - **Receita Bruta Total**: **R$ 338.523,10** (573 pedidos faturados).
  - **Reembolsos Concedidos**: **R$ 28.608,50** (48 pedidos devolvidos).
  - **Receita Líquida Real**: **R$ 309.914,60**.
  - **Ticket Médio**: R$ 590,79 por pedido.
  - **Taxa Global de Reembolso**: **8,38%**.
  - **Adesão ao Order Bump (`BUMP-MENTORIA`)**: **15,71%** (90 compras levaram a mentoria adicional em grupo), agregando receita pura sem custo de aquisição adicional.
  - **Jornada Lead → Compra**:
    - 522 pessoas baixaram o e-book gratuito.
    - 40 dessas pessoas efetuaram compra posterior (conversão de **7,66%**).
    - **Mediana de tempo de conversão**: **4,64 dias**.
- **Canais com Menor Desistência (Menor Taxa de Reembolso)**:
  - Canais orgânicos (`google / organic`) e acessos diretos registraram as taxas de reembolso mais baixas (inferiores a 5%), demonstrando maior maturidade e convicção de compra. Em contrapartida, canais de anúncios de display e redes sociais registraram taxas ligeiramente superiores a 10%.
- **Recomendação Acionável**:
  1. Estruturar uma sequência automatizada de e-mail marketing focada em nutrição agressiva entre o 3º e o 5º dia pós-download do e-book, alinhando-se exatamente à mediana de maturação de 4,64 dias.
  2. Implementar onboarding acolhedor e liberação imediata de módulos práticos nos primeiros 7 dias para combater os estornos no prazo de garantia.

---

### Pergunta 4: Previsão e Segmentação
> **Quais sessões estão perto de comprar, e que perfis de visitante existem?**

- **Tabelas de Sustentação**: [`gold.propensao_sessao`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_ml.py), [`gold.metricas_modelo`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_ml.py), [`gold.segmentos_visitantes`](file:///C:/Users/user/OneDrive/Área de Trabalho/Computacao_nuvem/big_data/projeto-final-aluno (1)/pipeline/gold_ml.py)
- **Evidências Numéricas**:
  - **Modelo de Machine Learning de Propensão**:
    - Algoritmo: Regressão Logística com regularização, estritamente livre de vazamento temporal (*data leakage* proibindo eventos de checkout).
    - **AUC-ROC no Teste**: **0.9176** (capacidade de discriminação excepcional).
    - **F1-Score**: **0.9045**.
    - Features de maior peso: rolagem profunda na landing page, visualização de planos, consumo de mais de 50% do VSL e buscas/downloads.
  - **Segmentação Comportamental (KMeans k=3)**:
    - **Cluster 0 — Compradores e Alta Intenção** (533 pessoas): 100% de taxa de conversão em compra, 1,91 sessões médias, alta profundidade de rolagem (70,14%) e navegação direcionada.
    - **Cluster 1 — Leads Engajados / Consumidores de Conteúdo** (522 pessoas): 100% deixaram e-mail para baixar e-book, maior engajamento geral (rolagem média de 78,47% e consumo de vídeo de 28,71%), com 7,66% convertendo em venda.
    - **Cluster 2 — Visitantes Casuais / Baixo Engajamento** (4.939 pessoas): 0% de conversão, poucas sessões (1,38) e rolagem superficial (59,15%).
- **Recomendação Acionável**:
  1. Integrar o escore de propensão em tempo real: para sessões com probabilidade estimada acima de 0,60 que demonstrem intenção de saída na landing page, disparar gatilho de chat comercial ou cupom de desconto com tempo limitado (15 minutos).
  2. Direcionar campanhas de remarketing do Cluster 1 focadas no conteúdo do curso e casos de sucesso, já que possuem alto engajamento prévio.

---

## 3. Qualidade do Dado e Engenharia de Confiabilidade

### Métricas de Higienização
- **Volume Bruto Ingerido na Landing**: 64.372 eventos distribuídos em 1.381 arquivos particionados.
- **Duplicatas Identificadas e Descartadas (R5)**: **656 eventos duplicados** eliminados via deduplicação por chave unívoca `event_id`.
- **Eventos Rejeitados por Violação de Contrato de Dados (R7)**: **1.903 eventos** isolados em `silver/eventos_rejeitados/`.
  - `timestamp_fora_da_faixa`: 296 eventos (datas inconsistentes anteriores a 01/09/2026 ou posteriores à data do lote de coleta).
  - `duracao_video_invalida`: 321 eventos (durações negativas ou inconsistentes com o player).
  - `percentual_video_invalido`: 319 eventos (valores percentuais fora do intervalo válido `[0, 100]`).
  - `valor_compra_invalido`: 330 eventos (valores monetários nulos, negativos ou zerados em transações de checkout).
  - `scroll_invalido`: 315 eventos (rolagens inferiores a 0 ou superiores a 100%).
  - `evento_desconhecido`: 322 eventos (tipos de evento desconhecidos fora da especificação).
- **Eventos Válidos e Consolidados na Silver**: **61.813 eventos**.
- **Itens Granulares Desaninhados em `silver/itens/` (R10)**: **24.427 itens**.

---

### Armadilhas Ocultas Identificadas e Tratamento Aplicado

1. **Armadilha do `session_hint` e Falsa Sessionização**:
   - *Problema*: Os dados brutos continham um campo `session_hint`. Utilizá-lo agruparia eventos incorretamente, pois o tracking web real sofre interrupções e reaberturas. O contrato exigia explicitamente não confiar nesse campo.
   - *Tratamento*: Implementação de sessionização analítica baseada em janela de 30 minutos de inatividade via funções de janela do Spark (`lag(event_ts)`, cálculo da diferença em segundos e soma cumulativa de novas sessões).

2. **Armadilha do Visitante Anônimo que Realiza Login Posteriormente (*Identity Stitching*)**:
   - *Problema*: Usuários navegam anonimamente (`user_pseudo_id`) em sessões anteriores e só se autenticam (`user_id`) no momento do lead ou compra. Se não tratados, suas ações anteriores constariam como de usuários desconhecidos.
   - *Tratamento*: Construção de mapa de identidade associando o primeiro `user_id` conhecido ao `user_pseudo_id` e enriquecendo retroativamente todos os eventos anteriores da mesma máquina/navegador.

3. **Armadilha do Vazamento de Dados no Modelo de Machine Learning (*Data Leakage*)**:
   - *Problema*: Eventos de etapas de checkout (`view_cart`, `begin_checkout`, `add_payment_info`) ocorrem apenas em quem já está no processo de fechamento. Incluí-los no modelo geraria um classificador com acurácia artificialmente perfeita, porém inútil para intervenções preditivas precoces.
   - *Tratamento*: Aplicação de corte temporal estrito (`event_ts < min(ts_add_to_cart)`), garantindo que as variáveis preditoras reflitam unicamente ações ocorridas na landing page antes da colocação no carrinho.

4. **Armadilha do Modo ANSI Nativo no PySpark 4.2**:
   - *Problema*: O PySpark 4 opera em modo ANSI por padrão. Tentativas de conversão de datas ISO com separador `T` via `unix_timestamp` ou divisões por denominadores zerados provocam interrupção fatal do job (`SparkDateTimeException` / `DIVIDE_BY_ZERO`).
   - *Tratamento*: Utilização de `F.to_timestamp(col).cast("long")` para manipulação robusta de ISO 8601 e uso de `F.try_divide` em todas as razões e taxas analíticas.

5. **Armadilha da Sobrescrita Destrutiva no S3**:
   - *Problema*: Gravar dados particionados na raiz `silver/` utilizando `mode("overwrite")` tradicional elimina sumariamente as pastas vizinhas (`silver/itens/` e `silver/eventos_rejeitados/`).
   - *Tratamento*: Configuração explícita de `partitionOverwriteMode = dynamic` em todas as rotinas de escrita da Silver, garantindo a substituição exclusiva das partições de eventos sem afetar diretórios adjacentes.
