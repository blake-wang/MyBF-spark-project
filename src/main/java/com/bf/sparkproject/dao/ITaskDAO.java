package com.bf.sparkproject.dao;

import com.bf.sparkproject.domain.Task;

/**
 * Created by wanglei on 2018/4/9.
 * 任务管理DAO接口
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     *
     * @param taskid
     * @return
     */
    Task findById(long taskid);

}
