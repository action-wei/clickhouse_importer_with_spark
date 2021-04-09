#!/bin/sh

. /etc/profile
. ~/.bash_profile

# status data hdfs
HdfsStatusData="hdfs://ns1018/user/jd_ad/ads_bgn/ad_bgn.db/dt_sku_service" 

# init value
LastDate="20190705"
ErrorNo=0

NewDate=`date -d "$(hadoop fs -cat ${HdfsStatusData})" +%Y%m%d`

# clear data two weeks ago
NeedClearDataDate=`date -d "14 days ago ${NewDate}" +%Y%m%d`

if [ -e last_data_date.txt ]; then
    LastDate=$(cat last_data_date.txt)
else
    echo ${NewDate} > last_data_date.txt ;
fi

if [[ ${NewDate} > ${LastDate} ]]; then
    # before create table, drop it if the target table have existed
    sh -x drop_bgn_table.sh ${NewDate}
    # create table
    sh -x create_bgn_data_table.sh ${NewDate}
    if [ $? -eq 0 ]; then
        # import data
        sh -x importer_bgn_data.sh ${NewDate}
        if [ $? -eq 0 ]; then
            echo ${NewDate} > last_data_date.txt ;
            # drop history table
            sh -x drop_bgn_table.sh ${NeedClearDataDate}
            ErrorNo=$?
        else
            ErrorNo=$?
        fi
    else
        ErrorNo=$?
    fi
fi

exit ${ErrorNo};




