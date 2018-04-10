package com.bf.sparkproject.dao.impl;

import com.bf.sparkproject.dao.ITaskDAO;
import com.bf.sparkproject.domain.Task;
import com.bf.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by wanglei on 2018/4/9.
 */
public class TaskDAOImpl implements ITaskDAO {

    /**
     * 根据主键查询任务
     *
     * @param taskid
     * @return
     */

    @Override
    public Task findById(long taskid) {
        final Task task = new Task();
        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long task_id = rs.getLong(1);
                    String task_name = rs.getString(2);
                    String create_time = rs.getString(3);
                    String start_time = rs.getString(4);
                    String finish_time = rs.getString(5);
                    String task_type = rs.getString(6);
                    String task_status = rs.getString(7);
                    String task_param = rs.getString(8);

                    task.setTask_id(task_id);
                    task.setTask_name(task_name);
                    task.setCreate_time(create_time);
                    task.setStart_time(start_time);
                    task.setFinish_time(finish_time);
                    task.setTask_type(task_type);
                    task.setTask_status(task_status);
                    task.setTask_param(task_param);
                }
            }
        });

        /**
         * 说在后面的话
         *
         * 大家看这个代码，包括后面的其他的DAO，就会发现，用JDBC进行数据库操作，最大的问题就是麻烦，
         * 你为了查询某些数据，需要自己编写大量的Domain对象的封装，数据的获取，数据的设置，造成大量很冗余的代码
         *
         * 所以说，之前就是说，不建议用Scala来开发大型复杂的Spark的工程项目
         * 因为大型复杂的工程项目，必定是要涉及很多第三方的东西的，MySql只是最基础的，要进行数据库操作可能还会有其他的redis，zookeeper等等
         *
         * 如果你就用Scala，那么势必会造成与调用第三方组件的代码用java，那么就会编程scala+java混编，大大降低我们的代码的开发和维护的效率
         *
         * 此外，即使，你是用了scala+java混编
         * 但是，真正最方便的，还是使用一些j2ee的开源框架，来进行第三方技术的整合和操作，比如MySql，那么可以用MyBatis/Hibernate，大大减少我们的冗余的代码
         * 大大提升我们的开发速度和效率
         *
         * 但是如果用了scala，那么用j2ee开源框架，造成scala+java+j2ee开源框架混编
         *
         *
         */

        return task;
    }

}

