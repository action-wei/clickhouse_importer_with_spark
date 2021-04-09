#!/bin/sh

if [ -n "$1" -a -n "$2" ] ;then
  DataDate=$1
  DataType=$2
else
  echo "please input DataDate and DataType, DataType in [DayType, WeekType]"
  exit 1
fi

ErrNo=0

clickhouse_host="http://stargazedb_rw:tzHp7ir73RNNaA@ch.ckstargaze.svc.mjq2.vitess.n.jd.local:8123/"

# drop product_word2id
drop_word2id_table_sql=" drop table if exists graphcompute.bgn_product_word2id_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_word2id_all_table_sql=" drop table if exists graphcompute.bgn_product_word2id_${DataDate}_all  on cluster  stargazedb_ck_cluster; "

# drop sku_to_product_word
drop_sku_to_product_word_table_sql=" drop table if exists graphcompute.sku_to_product_words_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_sku_to_product_word_all_table_sql=" drop table if exists graphcompute.sku_to_product_words_${DataDate}_all  on cluster  stargazedb_ck_cluster;"

if [ "${DataType}" == "DayType" ]; then
  echo "${drop_word2id_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${drop_word2id_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${drop_sku_to_product_word_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${drop_sku_to_product_word_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
fi

# drop similar product_word
drop_similar_product_word_table_sql=" drop table if exists graphcompute.bgn_similar_product_words_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_similar_product_word_all_table_sql=" drop table if exists graphcompute.bgn_similar_product_words_${DataDate}_all  on cluster  stargazedb_ck_cluster;"

if [ "${DataType}" == "WeekType" ]; then
  echo "${drop_similar_product_word_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
  echo "${drop_similar_product_word_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
  let ErrNo+=$?
fi
exit ${ErrNo}
