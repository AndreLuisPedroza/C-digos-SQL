-- Databricks notebook source
create table venda_jogos(
id int,
nome_jogo varchar(50),
data_venda date,
valor_pagamento float

)

-- COMMAND ----------

insert into venda_jogos(id, nome_jogo, data_venda, valor_pagamento)
values(2, 'Sonic', '2022-05-10', 69.90)

-- COMMAND ----------

select * from venda_jogos

-- COMMAND ----------

insert into venda_jogos(id, nome_jogo, data_venda, valor_pagamento)
select 2, 'Sonic', '2022-05-10', 69.90

-- COMMAND ----------

insert into venda_jogos(id, nome_jogo, data_venda, valor_pagamento)
select 3, 'Fifa', '2022-05-10', 69.90
union all
select 4, 'God', '2022-05-10', 69.90
union all
select 5, 'GTA', '2022-05-10', 69.90


-- COMMAND ----------

select * from vendas_games

-- COMMAND ----------

select nome, plataforma
from vendas_games limit 10

-- COMMAND ----------

select * from vendas_games

-- COMMAND ----------

select distinct marca
from vendas_games

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Atualização dos Campos (Update)

-- COMMAND ----------

update vendas_games set marca = 'Activision'
where marca = 'Activision Blizzard'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from default.vendas_games")
-- MAGIC 
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("vendas_games_final")

-- COMMAND ----------

select * from vendas_games_final

-- COMMAND ----------

update vendas_games_final set marca = 'Activision'
where marca = 'Activision Blizzard'

-- COMMAND ----------

update vendas_games_final set marca = 'Activision'
where marca = 'Activision Value'

-- COMMAND ----------

drop table vendas_games

-- COMMAND ----------

drop table vendas_jogo;

-- COMMAND ----------

drop table vendas_games_new;

-- COMMAND ----------

-- MAGIC %md Deletando registros(delete)

-- COMMAND ----------

delete from vendas_games_final
where marca in(2000,
2005,
2006,
2007,
2008,
2009,
2010,
2011,
2015
)

-- COMMAND ----------

-- MAGIC %md Verificar se inconsistencias foram tratadas

-- COMMAND ----------

select distinct marca
from vendas_games_final

-- COMMAND ----------

-- MAGIC %md Comando Case

-- COMMAND ----------

select DISTINCT case when plataforma in ('PS2', 'PS3', 'PS4', 'PS5', 'PSP', 'PS') then 'Playstation'
               when plataforma in ('X360', 'XOne') then 'Xbox' 
          else plataforma end as plataforma_ajustada
from vendas_games_final
--- apenas pra essa consulta

-- COMMAND ----------

select DISTINCT genero
from vendas_games_final

-- COMMAND ----------

delete from vendas_games_final
where genero in (2016,
2014,
2013,
2005,
2000,
2002,
2009,
2006,
2011,
2008,
1994,
2007,
2015,
1998,
2001,
2010,
2003,
null
)

-- COMMAND ----------

-- MAGIC %md Comando Order By

-- COMMAND ----------

select * from vendas_games_final

-- COMMAND ----------

select nome, vendas_europa
from vendas_games_final

-- COMMAND ----------

select nome, vendas_europa
from vendas_games_final
order by vendas_europa

-- COMMAND ----------

select nome, vendas_europa
from vendas_games_final
order by vendas_europa desc

-- COMMAND ----------

select nome, cast (vendas_europa as float)
from vendas_games_final
where vendas_europa not in
(
'2008',
'2007'

)

order by vendas_europa desc

-- COMMAND ----------

-- MAGIC %md Agrupamento comando Group By

-- COMMAND ----------

---SUM = soma, MAX = Valor máximo, MIN = Valor mínimo, AVG = Média, COUNT ---
select plataforma, COUNT(DISTINCT NOME) AS QTD_JOGOS, SUM(cast(vendas_europa as float)) AS TOTAL_VENDAS_EUROPA, AVG(cast(vendas_eua_milhoes as float)) AS MEDIA_VENDAS_EUA, MAX(cast(vendas_japao_milhoes as float)) AS MAIOR_FATURAMENTO_JAPAO
from vendas_games_final
where vendas_europa not in
(
'2008',
'2007'

)

group by plataforma
order by SUM(cast(vendas_europa as float)) desc

-- COMMAND ----------

select * from vendas_games_final

-- COMMAND ----------

SELECT COUNT(DISTINCT plataforma)
FROM vendas_games_final

-- COMMAND ----------

-- MAGIC %md LEFT, RIGHT, REPLACE, ROUND, TRIM, RTRIM, LTRIM, CAST, SUBSTRING

-- COMMAND ----------

select plataforma, ROUND(SUM(cast(vendas_europa as float)),2) AS TOTAL_VENDAS_EUROPA
from vendas_games_final
WHERE vendas_europa NOT IN ('2008', '2007')
GROUP BY plataforma
ORDER BY SUM(CAST(vendas_europa AS float)) DESC

-- COMMAND ----------

-- MAGIC %md LEFT, RIGHT

-- COMMAND ----------

SELECT LEFT(vendas_europa, 2)
FROM vendas_games_final

-- COMMAND ----------

SELECT *
FROM vendas_games_final

-- COMMAND ----------

SELECT RIGHT(ano_lancamento, 2)
FROM vendas_games_final

-- COMMAND ----------

--- REPLACE ---
SELECT REPLACE(plataforma, 'PS2', 'Playstation')

FROM vendas_games_final

-- COMMAND ----------

SELECT CONCAT(ANO_LANCAMENTO, '_ANO')
FROM vendas_games_final

-- COMMAND ----------

-- MAGIC %md =, in, >, <, >=, <=, between, like e having

-- COMMAND ----------

SELECT *
FROM vendas_games_final
WHERE nome IN ('Wii Sports', 'Tetris')

-- COMMAND ----------

SELECT *
FROM vendas_games_final
WHERE vendas_europa BETWEEN 1.0 AND 1.5

-- COMMAND ----------

SELECT *
FROM vendas_games_final
WHERE nome LIKE '%FIFA%'

-- COMMAND ----------

select plataforma, ROUND(SUM(cast(vendas_europa as float)),2) AS TOTAL_VENDAS_EUROPA
from vendas_games_final
WHERE vendas_europa NOT IN ('2008', '2007')
GROUP BY plataforma
HAVING ROUND(SUM(cast(vendas_europa as float)),2) > 100.0
ORDER BY 2 DESC
