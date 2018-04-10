package com.bf.sparkproject.spark;

import com.bf.sparkproject.MockData;
import com.bf.sparkproject.constant.Constants;
import com.bf.sparkproject.conf.ConfigurationManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by wanglei on 2018/4/9.
 * 用户访问Session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期-结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索次：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQl的task表中，任务参数以JSON格式封装在task_param字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell 脚本
 * spark-submit shell脚本,在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是Spark本身提供的特性
 *
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

        //如果要进行session粒度的数据聚合，首先要从user_visit_action表中，查询出来指定日期范围内的行为数据





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
