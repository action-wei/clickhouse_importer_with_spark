#!/bin/sh

if [ -n "$1" ] ;then
  DataDate=$1
else
  echo "please input DataDate"
  exit 1
fi
clickhouse_host="http://stargazedb_rw:tzHp7ir73RNNaA@ch.ckstargaze.svc.mjq2.vitess.n.jd.local:8123/"

ErrNo=0

# drop vertex
drop_vertex_table_sql=" drop table if exists graphcompute.vertex_bgn_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_vertex_all_table_sql=" drop table if exists graphcompute.vertex_bgn_${DataDate}_all  on cluster  stargazedb_ck_cluster; "

echo "${drop_vertex_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_vertex_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# drop edge
drop_edge_table_sql=" drop table if exists graphcompute.edge_bgn_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_edge_all_table_sql=" drop table if exists graphcompute.edge_bgn_${DataDate}_all  on cluster  stargazedb_ck_cluster;"

echo "${drop_edge_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_edge_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# drop sku attr
drop_sku_attr_concept_table_sql=" drop table if exists graphcompute.sku_attr_concept_bgn_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_sku_attr_concept_all_table_sql=" drop table if exists graphcompute.sku_attr_concept_bgn_${DataDate}_all  on cluster  stargazedb_ck_cluster;"
drop_sku_attr_postag_table_sql=" drop table if exists graphcompute.sku_attr_pos_tag_bgn_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_sku_attr_postag_all_table_sql=" drop table if exists graphcompute.sku_attr_pos_tag_bgn_${DataDate}_all  on cluster  stargazedb_ck_cluster;"
drop_sku_attr_segment_table_sql=" drop table if exists graphcompute.sku_attr_segment_bgn_${DataDate}  on cluster  stargazedb_ck_cluster; "
drop_sku_attr_segment_all_table_sql=" drop table if exists graphcompute.sku_attr_segment_bgn_${DataDate}_all  on cluster  stargazedb_ck_cluster;"


echo "${drop_sku_attr_concept_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_sku_attr_concept_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_sku_attr_postag_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_sku_attr_postag_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_sku_attr_segment_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${drop_sku_attr_segment_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

exit ${ErrNo}