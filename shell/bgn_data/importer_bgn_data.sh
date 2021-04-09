#!/bin/sh

DataBase="graphcompute"
TableVersionDate=""
DataDate=""

if [ -n "$1" ] ;then
  DataDate=$1
else
  echo "please input DataDate"
  exit 1
fi

TableVersionDate=${DataDate}
DataDate=`date -d "${DataDate}" +%Y-%m-%d`

############### vertex ########################
VertexTableName="vertex_bgn_${TableVersionDate}"

# sku vertex
spark-submit \
    --class "clickhouse_importer.Vertex_Sku_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${VertexTableName} ${DataDate} > logs/vertex_sku.log 2>&1

# entity vertex
spark-submit \
    --class "clickhouse_importer.Vertex_Entity_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${VertexTableName} ${DataDate} > logs/vertex_entity.log 2>&1

# concept vertex
spark-submit \
    --class "clickhouse_importer.Vertex_Concept_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${VertexTableName} ${DataDate} > logs/vertex_concept.log 2>&1


#####################   sku attr  ##############################
# sku attr concept
SkuAttrConceptTableName="sku_attr_concept_bgn_${TableVersionDate}"

spark-submit \
    --class "clickhouse_importer.Sku_Attr_Concept_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${SkuAttrConceptTableName} ${DataDate} > logs/sku_attr_concept.log 2>&1

# sku attr pos-tag
SkuAttrPosTagTableName="sku_attr_pos_tag_bgn_${TableVersionDate}"
spark-submit \
    --class "clickhouse_importer.Sku_Attr_PosTag_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${SkuAttrPosTagTableName} ${DataDate} > logs/sku_attr_postag.log 2>&1

# sku attr segment
SkuAttrSegmentTableName="sku_attr_segment_bgn_${TableVersionDate}"
spark-submit \
    --class "clickhouse_importer.Sku_Attr_Segment_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${SkuAttrSegmentTableName} ${DataDate} > logs/sku_attr_segment.log 2>&1

################# edge  ####################
EdgeTableName="edge_bgn_${TableVersionDate}"

# concept-concept
spark-submit \
    --class "clickhouse_importer.Edge_Concept_Concept_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_concept_concept.log 2>&1

# concept-enity-blc
spark-submit \
    --class "clickhouse_importer.Edge_Concept_Entity_BLC_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_concept_entity_blc.log 2>&1

# concept-entity
spark-submit \
    --class "clickhouse_importer.Edge_Concept_Entity_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_concept_entity.log 2>&1

# entity-concept-blc
spark-submit \
    --class "clickhouse_importer.Edge_Entity_Concept_BLC_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_entity_concept_blc.log 2>&1

# entity-concept
spark-submit \
    --class "clickhouse_importer.Edge_Entity_Concept_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_entity_concept.log 2>&1

# entity-sku
spark-submit \
    --class "clickhouse_importer.Edge_Entity_Sku_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_entity_sku.log 2>&1

# sku-entity
spark-submit \
    --class "clickhouse_importer.Edge_Sku_Entity_Importer" \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --executor-memory 5g \
    --driver-memory 20g \
    --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
    clickhouse_importer_2.11-1.0.jar ${DataBase} ${EdgeTableName} ${DataDate} > logs/edge_sku_entity.log 2>&1