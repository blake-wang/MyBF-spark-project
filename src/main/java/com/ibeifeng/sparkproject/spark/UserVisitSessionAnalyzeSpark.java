package com.ibeifeng.sparkproject.spark;

import com.ibeifeng.sparkproject.MockData;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by wanglei on 2018/4/9.
 * 用户访问Session分析Spark作业
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //sc.sc()  从JavaSparkContext中取出它对应的那个SparkContext
        SQLContext sqlContext = getSQLContext(sc.sc());


        MockData md = new MockData();
        md.mock(sc, sqlContext);

        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_APP_NAME_SESSION);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }

    }

    /**
     * 生成模拟数据(只有本地模式，才会生成模拟数据)
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }

    }

}
