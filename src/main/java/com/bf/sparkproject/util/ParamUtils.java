package com.bf.sparkproject.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bf.sparkproject.conf.ConfigurationManager;
import com.bf.sparkproject.constant.Constants;

/**
 * 参数工具类
 *
 * @author Administrator
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务id
     *
     * @param args 命令行参数
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args, String taskType) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            return ConfigurationManager.getLong(taskType);
        } else {
            try {
                if (args != null && args.length > 0) {
                    return Long.valueOf(args[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return 0L;
    }

    /**
     * 从JSON对象中提取参数
     *
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }

    /**
     * 从命令行参数中提取任务ID
     * @param args
     * @return
     */
    public static Long getTaskIdFromArgs(String[] args) {
        try {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

}
