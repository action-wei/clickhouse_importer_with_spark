######### vertex
create table if not exists graphcompute.vertex_bgn_20190726 on cluster stargazedb_ck_cluster (`id` String, `type` String, `display` String, `is_attr` String, `fs_attr` String, `ss_attr` String, `bs_attr` String ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.vertex_bgn_20190726', '{replica}') partition by (type) order by (id);

create table if not exists graphcompute.vertex_bgn_20190726_all on cluster stargazedb_ck_cluster ( `id` String, `type` String, `display` String, `is_attr` String, `fs_attr` String, `ss_attr` String, `bs_attr` String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'vertex_bgn_20190726');


###########  sku concept
create table if not exists graphcompute.sku_attr_concept_bgn_20190726 on cluster stargazedb_ck_cluster (id String, sku_name String, concept_from_title String, concept_from_attribute String, main_product_name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_concept_bgn_20190726', '{replica}') order by (id);

create table if not exists graphcompute.sku_attr_concept_bgn_20190726_all on cluster stargazedb_ck_cluster (id String, sku_name String, concept_from_title String, concept_from_attribute String, main_product_name String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_concept_bgn_20190726');

############ sku pos_tag
create table if not exists graphcompute.sku_attr_pos_tag_bgn_20190726 on cluster stargazedb_ck_cluster (id String, sku_name String, pos_tag String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_pos_tag_bgn_20190726', '{replica}') order by (id);

create table if not exists graphcompute.sku_attr_pos_tag_bgn_20190726_all on cluster stargazedb_ck_cluster (id String, sku_name String, pos_tag String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_pos_tag_bgn_20190726');


#########  sku segment
create table if not exists graphcompute.sku_attr_segment_bgn_20190726 on cluster stargazedb_ck_cluster (id String, sku_name String, sku_segment String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_attr_segment_bgn_20190726', '{replica}') order by (id);

create table if not exists graphcompute.sku_attr_segment_bgn_20190726_all on cluster stargazedb_ck_cluster (id String, sku_name String, sku_segment String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_attr_segment_bgn_20190726');


########  edge
create table if not exists graphcompute.edge_bgn_20190726 on cluster stargazedb_ck_cluster (`v1` String, `v2` String, `w` Float32, `dir` Int8, `type` String, `display` String, `is_attr` String, `fs_attr` String, `ss_attr` String, `bs_attr` String ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.edge_bgn_20190726', '{replica}') partition by concat(splitByChar('_', v1)[1],'_', splitByChar('_',v2)[1]) order by (v1);


create table if not exists graphcompute.edge_bgn_20190726_all on cluster stargazedb_ck_cluster ( `v1` String, `v2` String, `w` Float32, `dir` Int8, `type` String, `display` String, `is_attr` String, `fs_attr` String, `ss_attr` String, `bs_attr` String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'edge_bgn_20190726');



############################### 产品词相关词表  #######################

####### 产品词id映射表
create table if not exists graphcompute.bgn_product_word2id_20190726 on cluster stargazedb_ck_cluster (id Int64, product_word String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.bgn_product_word2id_20190726', '{replica}') order by (id)

create table if not exists graphcompute.bgn_product_word2id_20190726_all on cluster stargazedb_ck_cluster (id Int64, product_word String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'bgn_product_word2id_20190726')


##### 相似产品词
create table if not exists graphcompute.bgn_similar_product_words_20190726 on cluster stargazedb_ck_cluster (word String, similar_words String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.bgn_similar_product_words_20190726', '{replica}') order by (word)

create table if not exists graphcompute.bgn_similar_product_words_20190726_all on cluster stargazedb_ck_cluster (word String, similar_words String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'bgn_similar_product_words_20190726')

#######  sku与产品词映射表
create table if not exists graphcompute.sku_to_product_words_20190726 on cluster stargazedb_ck_cluster (sku_id String, product_word_ids String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.sku_to_product_words_20190726', '{replica}') order by (sku_id)

create table if not exists graphcompute.sku_to_product_words_20190726_all on cluster stargazedb_ck_cluster (sku_id String, product_word_ids String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'sku_to_product_words_20190726')


#######  产品词上下位关系表
create table if not exists graphcompute.bgn_product_word_relation on cluster stargazedb_ck_cluster (prodcut_version String,category String,word String, relation String, word_list String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/graphcompute.bgn_product_word_relation', '{replica}') partition by (relation) order by (word)

create table if not exists graphcompute.bgn_product_word_relation_all on cluster stargazedb_ck_cluster (product_version String, category String, word String, relation String, word_list String) ENGINE = Distributed('stargazedb_ck_cluster', 'graphcompute', 'bgn_product_word_relation')
