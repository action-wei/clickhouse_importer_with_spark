#!/bin/sh

. /etc/profile
. ~/.bash_profile

# status data hdfs
HdfsStatusData="hdfs://ns1018/user/jd_ad/ads_bgn/ad_bgn.db/dt_sku_service" 

# init value
LastWeekDate="20190726"
LastDayDate="20190813"
ErrorNo=0

NewWeekDate=`date -d "$(hadoop fs -cat ${HdfsStatusData})" +%Y%m%d`
NewDayDate=`date -d "2 days ago" +%Y%m%d`

# clear data
NeedToClearDayDate=`date -d "2 days ago ${NewDayDate}" +%Y%m%d`
NeedToClearWeekDate=`date -d "14 days ago ${NewWeekDate}" +%Y%m%d`

# weekly data
if [ -e last_week_data_date.txt ]; then
    LastWeekDate=$(cat last_week_data_date.txt)
else
    echo ${NewWeekDate} > last_week_data_date.txt ;
fi

# dayly data
if [ -e last_data_date.txt ]; then
    LastDayDate=$(cat last_data_date.txt)
else
    echo ${NewDayDate} > last_data_date.txt ;
fi

DataType="DayType"
if [[ ${NewDayDate} > ${LastDayDate} ]]; then
    # drop table if have existed
    sh -x drop_product_word_table.sh ${NewDayDate} ${DataType}
    # create table
    sh -x create_product_word_table.sh ${NewDayDate} ${DataType}
    if [ $? -eq 0 ]; then
        # import data
        sh -x importer_product_word_data.sh ${NewDayDate} ${DataType}
        if [ $? -eq 0 ]; then
            echo ${NewDayDate} > last_data_date.txt ;
            # drop history table
            sh -x drop_product_word_table.sh ${NeedToClearDayDate} ${DataType}
            ErrorNo=$?
        else
            ErrorNo=$?
        fi
    else
        ErrorNo=$?
    fi
fi

DataType="WeekType"
if [[ ${NewWeekDate} > ${LastWeekDate} ]]; then
    # drop table if have existed
    sh -x drop_product_word_table.sh ${NewWeekDate} ${DataType}
    # create table
    sh -x create_product_word_table.sh ${NewWeekDate} ${DataType}
    if [ $? -eq 0 ]; then
        # import data
        sh -x importer_product_word_data.sh ${NewWeekDate} ${DataType}
        if [ $? -eq 0 ]; then
            echo ${NewWeekDate} > last_week_data_date.txt ;
            # drop history table
            sh -x drop_product_word_table.sh ${NeedToClearWeekDate} ${DataType}
            ErrorNo=$?
        else
            ErrorNo=$?
        fi
    else
        ErrorNo=$?
    fi
fi

exit ${ErrorNo};




