package com.bf.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.bf.sparkproject.MockData;
import com.bf.sparkproject.conf.ConfigurationManager;
import com.bf.sparkproject.constant.Constants;
import com.bf.sparkproject.dao.ITaskDAO;
import com.bf.sparkproject.dao.impl.DAOFactory;
import com.bf.sparkproject.domain.Task;
import com.bf.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

/**
 * Created by wanglei on 2018/4/9.
 * 用户访问Session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期-结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索次：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * 我们的spark作业如何接受用户创建的任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQl的task表中，任务参数以JSON格式封装在task_param字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell 脚本
 * spark-submit shell脚本,在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * <p>
 * 这是Spark本身提供的特性
 */

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //sc.sc()  从JavaSparkContext中取出它对应的那个SparkContext
        SQLContext sqlContext = getSQLContext(sc.sc());


        //生成模拟测试数据
        mockData(sc, sqlContext);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();


        //那么就首先得查询出来指定的任务
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTask_param());

        //如果要进行session粒度的数据聚合
        //首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        //如果要根据用户在创建任务时指定的参数，来进行数据过滤和筛选
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        //首先，可以将行为数据，按照session_id进行groupByKey分组
        //此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        //与用户信息数据，进行join
        //然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息


        //关闭上下文
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

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "' ";

        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();

    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {
        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //我们现在需要将这个Row映射成<sessionid,Row>的格式

        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long,String> userid2PairAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                return null;
            }
        });


        return null;
    }

}
