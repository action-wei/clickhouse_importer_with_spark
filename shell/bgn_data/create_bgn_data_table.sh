#!/bin/sh

# Date
if [ -n "$1" ] ;then
  DataDate=$1
else
  echo "please input DataDate"
  exit 1
fi

ErrNo=0

clickhouse_host="http://stargazedb_rw:tzHp7ir73RNNaA@ch.ckstargaze.svc.mjq2.vitess.n.jd.local:8123/"

create_vertex_table_sql=" \
create table if not exists graphcompute.vertex_bgn_${DataDate} \
on cluster stargazedb_ck_cluster (id String, type String, display String, is_attr String, fs_attr String, ss_attr String, bs_attr String, spec_attr String, ext_attr String ) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.vertex_bgn_${DataDate}', '{replica}') partition by (type) order by (id); \
"

create_vertex_all_table_sql=" \
create table if not exists graphcompute.vertex_bgn_${DataDate}_all \
on cluster stargazedb_ck_cluster ( id String, type String, display String, is_attr String, fs_attr String, ss_attr String, bs_attr String, spec_attr String, ext_attr String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'vertex_bgn_${DataDate}');"

echo "${create_vertex_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${create_vertex_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# sku attr concept
create_sku_attr_concept_table_sql=" \
create table if not exists graphcompute.sku_attr_concept_bgn_${DataDate} \
on cluster stargazedb_ck_cluster \
(id String, sku_name String, concept_from_title String, concept_from_attribute String, main_product_name String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_concept_bgn_${DataDate}', '{replica}') order by (id); \
"

create_sku_attr_concept_all_table_sql=" \
create table if not exists graphcompute.sku_attr_concept_bgn_${DataDate}_all \
on cluster stargazedb_ck_cluster \
(id String, sku_name String, concept_from_title String, concept_from_attribute String, main_product_name String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_concept_bgn_${DataDate}'); \
"

echo "${create_sku_attr_concept_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${create_sku_attr_concept_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# sku attr pos_tag
create_sku_attr_pos_tag_table_sql=" \
create table if not exists graphcompute.sku_attr_pos_tag_bgn_${DataDate} \
on cluster stargazedb_ck_cluster \
(id String, sku_name String, pos_tag String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_pos_tag_bgn_${DataDate}', '{replica}') order by (id); \
"

create_sku_attr_pos_tag_all_table_sql=" \
create table if not exists graphcompute.sku_attr_pos_tag_bgn_${DataDate}_all \
on cluster stargazedb_ck_cluster
(id String, sku_name String, pos_tag String)
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_pos_tag_bgn_${DataDate}');
"

echo "${create_sku_attr_pos_tag_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${create_sku_attr_pos_tag_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# sku attr segment
create_sku_attr_segment_table_sql=" \
create table if not exists graphcompute.sku_attr_segment_bgn_${DataDate} \
on cluster stargazedb_ck_cluster \
(id String, sku_name String, sku_segment String) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_segment_bgn_${DataDate}', '{replica}') order by (id); \
"

create_sku_attr_segment_all_table_sql=" \
create table if not exists graphcompute.sku_attr_segment_bgn_${DataDate}_all \
on cluster stargazedb_ck_cluster \
(id String, sku_name String, sku_segment String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_segment_bgn_${DataDate}'); \
"

echo "${create_sku_attr_segment_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${create_sku_attr_segment_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

# edge
create_edge_table_sql=" \
create table if not exists graphcompute.edge_bgn_${DataDate} \
on cluster stargazedb_ck_cluster \
(v1 String, v2 String, w Float32, dir Int8, type String, display String, is_attr String, fs_attr String, ss_attr String, bs_attr String ) \
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.edge_bgn_${DataDate}', '{replica}') partition by concat(splitByChar('_', v1)[1],'_', splitByChar('_',v2)[1]) order by (v1); \
"

create_edge_all_table_sql=" \
create table if not exists graphcompute.edge_bgn_${DataDate}_all \
on cluster stargazedb_ck_cluster \
( v1 String, v2 String, w Float32, dir Int8, type String, display String, is_attr String, fs_attr String, ss_attr String, bs_attr String) \
ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'edge_bgn_${DataDate}'); \
"

echo "${create_edge_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?
echo "${create_edge_all_table_sql}" | curl "${clickhouse_host}" --data-binary @-
let ErrNo+=$?

exit ${ErrNo}