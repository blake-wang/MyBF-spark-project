package com.ibeifeng.sparkproject.jdbc;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

/**
 * Created by wanglei on 2018/4/9.
 * JDBC辅助组件
 * 在正式的项目的代码编写过程中，是完全严格按照大公司的coding标准来的
 * 也就是说，在代码中，是不能出现任何hard code(硬编码)的字符
 * 比如"张三","com.mysql.jdbc.Driver"
 * 所有这些东西，都需要通过常量来封装和使用
 */
public class JDBCHelper {
    //第一步：在静态代码块中，直接加载数据库的驱动
    //加载驱动，不能直接简单的，使用com.mysql.jdbc.Driver就可以了
    //之所以说，不要硬编码，他的原因就在于这里
    //com.mysql.jdbc.Driver只代表了MySql数据库的驱动
    //那么，如果有一天，我们的项目底层的数据库要进行迁移，比如迁移到Oracle，或者是DB2,SQLServer
    //那么，就必须很费劲的在代码中找，找到硬编码了com.mysql.jdbc.Driver的地方，然后改成其他数据库的驱动类的类名
    //所以正规项目，是不允许硬编码的，那样维护成本很高

    //通常，我们都是用一个常量接口中的某个常量，来代表一个值
    //然后在这个值改变的时候，只要改变常量接口中的常量对应的值就可以了。

    //项目，要尽量做成可配置的
    //就是说，我们的这个数据库驱动，更进一步，也不只是放在常量接口就可以了
    //最好的方式，是放在外部的配置文件中，跟代码彻底分离
    //常量接口中，只是包含了这个值对应的key的名字

    static {
        try {
            Class.forName(ConfigurationManager.getProperty(Constants.JDNC_DRIVER));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


}
