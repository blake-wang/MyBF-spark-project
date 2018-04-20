package com.bf.sparkproject.util;

import com.bf.sparkproject.MockData;
import com.bf.sparkproject.conf.ConfigurationManager;
import com.bf.sparkproject.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Spark工具类
 */
public class SparkUtils {
    /**
     * 根据当前是否本地测试的配置
     *
     * @param conf
     */
    public static void setMaster(SparkConf conf) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据
     * 如果spark.local的配置设置为true，则生成模拟数据，否则不生成
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc,sqlContext);
        }

    }


}
