# Rascunho do post do LinkedIn

Três versões. A primeira é a recomendada: abre com um problema técnico concreto em vez de
listar ferramentas — é o que diferencia post de portfólio de post de currículo.

Ajuste o link do repositório antes de publicar.

---

## Versão 1 — recomendada (abre pelo problema)

> Passei o fim de semana montando um pipeline de dados em tempo real da B3 e a primeira
> descoberta foi desconfortável: **não existe feed WebSocket gratuito da bolsa brasileira**.
> Dado real-time de bolsa é produto pago.
>
> Muita gente resolveria isso escondendo o problema. Eu preferi medir.
>
> Fui olhar os números da API gratuita que usei (brapi.dev): 15.000 requisições/mês, 1 ticker
> por chamada, cotação com ~30 minutos de atraso. Polling de 15 segundos em 4 papéis daria
> ~690.000 requisições/mês — a cota estouraria em duas horas.
>
> O que fiz com isso:
>
> → Implementei o streaming como polling de alta frequência, publicando cada leitura no Kafka.
> A arquitetura (Kafka → Spark Structured Streaming → Delta Lake) é exatamente a mesma de um
> cenário com feed real de bolsa: trocar o producer não exige tocar em nenhuma outra camada.
>
> → Transformei o atraso em métrica. Cada linha carrega `feed_delay_seconds` =
> horário de ingestão − horário da cotação na origem. O dashboard mostra esse atraso.
> Saber o quanto o dado está velho é requisito de qualquer mesa; ignorar isso é o erro.
>
> → O polling gera duplicata: o feed atualiza a cada ~30 min, então várias leituras devolvem
> exatamente a mesma cotação. A bronze guarda tudo (auditoria), a silver deduplica por chave
> natural com MERGE incremental no Delta. Bronze é o que aconteceu; silver é a verdade.
>
> → E um detalhe de domínio que quase passou batido: a API entrega volume ACUMULADO do dia.
> Jogar isso direto num gráfico de barras estaria errado. O volume por janela é reconstruído
> por diferença contra a janela anterior.
>
> O que subiu no fim: Kafka em modo KRaft (sem Zookeeper), PySpark Structured Streaming,
> Delta Lake com bronze/silver/gold, dbt-core com 50+ testes de qualidade, Airflow orquestrando
> batch e alertas, e um dashboard Streamlit com cara de terminal financeiro. Seis containers,
> um `docker compose up`, custo R$ 0.
>
> Bônus que me surpreendeu: rodei a validação de acessibilidade da paleta antes de desenhar o
> gráfico e o verde/vermelho tradicional de terminal financeiro FALHOU o teste de daltonismo
> (ΔE 4.1 em deuteranopia). Troquei o par e mantive seta + sinal como codificação secundária.
>
> Código, arquitetura e — principalmente — a seção de limitações conhecidas:
> 🔗 https://github.com/MateusSouza74/b3-realtime-lakehouse
>
> Um portfólio sem seção de limitações é propaganda, não engenharia.
>
> #DataEngineering #ApacheKafka #ApacheSpark #DeltaLake #dbt #ApacheAirflow #Python
> #MercadoFinanceiro #B3 #Dados

---

## Versão 2 — curta (para quem lê no celular)

> Não existe feed WebSocket gratuito da B3. Dado real-time de bolsa é pago.
>
> Em vez de esconder essa limitação, construí um pipeline que a MEDE: cada cotação carrega
> `feed_delay_seconds` (ingestão − horário na origem), e o dashboard mostra o atraso do feed.
>
> Stack: Kafka (KRaft) → PySpark Structured Streaming → Delta Lake (bronze/silver/gold) → dbt
> com 50+ testes → Airflow → Streamlit estilo terminal financeiro.
> Seis containers, um comando, R$ 0.
>
> Três coisas que aprendi no caminho:
> • polling gera duplicata → bronze audita, silver deduplica com MERGE
> • a API entrega volume ACUMULADO → volume por janela precisa ser reconstruído por diferença
> • média móvel não pode atravessar o fechamento de um pregão para o outro
>
> 🔗 https://github.com/MateusSouza74/b3-realtime-lakehouse
>
> #DataEngineering #Kafka #Spark #DeltaLake #dbt #Airflow #B3

---

## Versão 3 — foco em qualidade de dados (bom para vaga em banco/seguradora)

> Num pipeline de dados financeiros, o teste que mais me ajudou não foi de preço. Foi de fuso
> horário.
>
> Erro de timezone é a falha mais silenciosa de pipeline financeiro: o dado parece certo, mas o
> candle cai na janela errada. A API devolve UTC, o pregão é America/Sao_Paulo, e ninguém
> percebe até alguém questionar o gráfico.
>
> Montei um lakehouse em tempo real da B3 (Kafka → Spark Structured Streaming → Delta → dbt →
> Airflow) e o que mais me deu trabalho — no bom sentido — foi a camada de qualidade:
>
> • `assert_no_future_quotes` → cotação com horário no futuro = bug de fuso
> • `assert_candles_ohlc_coherent` → máxima < fechamento pega erro de agregação antes de o
>   gráfico mentir
> • `assert_bronze_stream_is_fresh` → pipeline parado há mais de 20 min dispara alerta
> • variação diária fora de ±30% → a B3 aciona circuit breaker em -10%, então isso é erro de
>   dado, não movimento de mercado
> • `relationships` do ticker contra a tabela de referência → contrato de dado: papel novo sem
>   cadastro quebra o build de propósito
>
> 50+ testes rodando dentro do `dbt build`, na mesma sessão Spark dos modelos: se a silver falha
> no teste, a gold não começa.
>
> 🔗 https://github.com/MateusSouza74/b3-realtime-lakehouse
>
> #DataEngineering #DataQuality #dbt #DeltaLake #Airflow #MercadoFinanceiro

---

## Checklist antes de publicar

- [x] Trocar o placeholder de usuário do GitHub (no post **e** no README)
- [ ] Subir os prints em `docs/images/` — post com imagem tem alcance muito maior
- [ ] Usar o **GIF** como mídia principal: mostrar o dashboard atualizando sozinho é o que prova
      que é streaming de verdade
- [ ] Gerar os prints entre 10h e 18h de um dia útil, com `SOURCE_MODE=brapi`, para aparecer
      dado real do pregão
- [ ] Deixar o repositório público e com descrição preenchida
- [ ] Adicionar os *topics* no GitHub: `data-engineering`, `apache-kafka`, `apache-spark`,
      `delta-lake`, `dbt`, `apache-airflow`, `streamlit`, `b3`
- [ ] Publicar terça, quarta ou quinta, entre 8h e 10h (BRT)
- [ ] Responder os comentários técnicos nas primeiras 2 horas — é o que sustenta o alcance
- [ ] Fixar o post no seu perfil

## O que responder quando perguntarem

**"Por que não usou Databricks / Snowflake / cloud?"**
O objetivo era que qualquer pessoa rodasse o projeto inteiro sem cartão de crédito e sem conta
em nenhum provedor. dbt-spark sobre Delta é a mesma linguagem de um projeto Databricks — o
adapter muda, o SQL não.

**"Polling não é streaming de verdade."**
Correto, e o README diz isso na primeira seção. A B3 não oferece WebSocket gratuito. A
arquitetura de streaming é a mesma; só a origem dos eventos mudaria.

**"Por que Airflow com SQLite?"**
Escopo local. Escrita local em Delta e metastore Derby embutido não ganham nada com paralelismo
e ganham risco de contenção — aqui a serialização é garantia, não limitação. Em produção seriam
Postgres e Celery/Kubernetes Executor, e está no roadmap dizer isso.
