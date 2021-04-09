#!/bin/sh

# Date
if [ -n "$1" -a -n "$2" ] ;then
  DataDate=$1
  DataType=$2
else
  echo "please input DataDate and DataType, DataType in [DayType, WeekType]"
  exit 1
fi

ErrNo=0

clickhouse_host="http://stargazedb_rw:tzHp7ir73RNNaA@ch.ckstargaze.svc.mjq2.vitess.n.jd.local:8123/"

# product word to id
create_word2id_table_sql=" \
create table if not exists graphcompute.bgn_product_word2id_${DataDate} \
on cluster stargazedb_ck_cluster \
(id Int64, product_word String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.bgn_product_word2id_${DataDate}', '{replica}') order by (id) \
"

create_word2id_all_table_sql=" \
create table if not exists graphcompute.bgn_product_word2id_${DataDate}_all \
on cluster stargazedb_ck_cluster (id Int64, product_word String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'bgn_product_word2id_${DataDate}') \
"

# sku to word
create_sku_to_word_table_sql=" \
create table if not exists graphcompute.sku_to_product_words_${DataDate} \
on cluster stargazedb_ck_cluster (sku_id String, product_word_ids String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_to_product_words_${DataDate}', '{replica}') order by (sku_id) \
"

create_sku_to_word_all_table_sql=" \
create table if not exists graphcompute.sku_to_product_words_${DataDate}_all \
on cluster stargazedb_ck_cluster (sku_id String, product_word_ids String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_to_product_words_${DataDate}') \
"

if [ "${DataType}" == "DayType" ]; then
  echo "${create_word2id_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${create_word2id_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${create_sku_to_word_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${create_sku_to_word_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
fi

# similar_product_word
create_similar_word_table_sql=" \
create table if not exists graphcompute.bgn_similar_product_words_${DataDate} \
on cluster stargazedb_ck_cluster (word String, similar_words String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.bgn_similar_product_words_${DataDate}', '{replica}') order by (word) \
"

create_similar_word_all_table_sql=" \
create table if not exists graphcompute.bgn_similar_product_words_${DataDate}_all \
on cluster stargazedb_ck_cluster (word String, similar_words String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'bgn_similar_product_words_${DataDate}') \
"

if [ "${DataType}" == "WeekType" ]; then
  echo "${create_similar_word_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${create_similar_word_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
fi

exit ${ErrNo}
