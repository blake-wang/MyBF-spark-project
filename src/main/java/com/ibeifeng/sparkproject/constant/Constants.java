package com.ibeifeng.sparkproject.constant;

/**
 * Created by wanglei on 2018/4/9.
 * <p>
 * 常量接口
 */
public interface Constants {
    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";

    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_LOCAL="spark.local";
}
