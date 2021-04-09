#!/bin/sh

DataBase="graphcompute"
TableVersionDate=""
DataDate=""
DataType=""

if [ -n "$1" -a -n "$2" ] ;then
  DataDate=$1
  DataType=$2
else
  echo "please input DataDate and DataType, DataType in [DayType, WeekType]"
  exit 1
fi

TableVersionDate=${DataDate}
DataDate=`date -d "${DataDate}" +%Y-%m-%d`

if [ "${DataType}" == "DayType" ];then
    ### sku_to_product_words_table
    SkuToProductWordsTableName="sku_to_product_words_${TableVersionDate}"

    spark-submit \
        --class "clickhouse_importer.Sku_To_Product_Words_Importer" \
        --master yarn \
        --deploy-mode client \
        --num-executors 5 \
        --executor-cores 10 \
        --executor-memory 5g \
        --driver-memory 10g \
        --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
        clickhouse_importer_2.11-1.0.jar ${DataBase} ${SkuToProductWordsTableName} ${DataDate} > logs/sku_to_product_word.log 2>&1 

    ### product_word2id table
    ProductWord2idTableName="bgn_product_word2id_${TableVersionDate}"
    InputPath="/user/jd_ad/ads_bgn/yanhan15/online/pos_tag_to_skuservice/${DataDate}/word2id/*"

    spark-submit \
        --class "clickhouse_importer.Product_Words2id_Importer" \
        --master yarn \
        --deploy-mode client \
        --num-executors 5 \
        --executor-cores 10 \
        --executor-memory 5g \
        --driver-memory 20g \
        --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
        clickhouse_importer_2.11-1.0.jar ${DataBase} ${ProductWord2idTableName} ${InputPath} > logs/product_word2id.log 2>&1
fi

if [ "${DataType}" == "WeekType" ];then
    SimilarProductWordsTableName="bgn_similar_product_words_${TableVersionDate}"
    InputPath="/user/jd_ad/ads_bgn/ad_bgn.db/cate_product_simlist/dt=${DataDate}_84/*/*"

    spark-submit \
        --class "clickhouse_importer.Similar_Product_Words_Importer" \
        --master yarn \
        --deploy-mode client \
        --num-executors 5 \
        --executor-cores 10 \
        --executor-memory 5g \
        --driver-memory 20g \
        --jars libs/clickhouse-jdbc-0.1.39.jar,libs/guava-19.0.jar \
        clickhouse_importer_2.11-1.0.jar ${DataBase} ${SimilarProductWordsTableName} ${InputPath} > logs/similar_product_word.log 2>&1
fi
