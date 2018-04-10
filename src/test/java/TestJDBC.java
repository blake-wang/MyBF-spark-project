import com.bf.sparkproject.jdbc.JDBCHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanglei on 2018/4/9.
 */
public class TestJDBC {
    public static void main(String[] args) {

        JDBCHelper jdbc = JDBCHelper.getInstance();
//        testQuery(jdbc);
//        testUpdate(jdbc);
//        testBatch(jdbc);


    }


    private static void testBatch(JDBCHelper jdbc) {
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(new Object[]{14, "柯泉", 27});
        list.add(new Object[]{15, "武广林", 39});
        int[] ints = jdbc.executeBatch("insert into user (id,name,age) values (?,?,?)", list);
        int length = ints.length;
        System.out.println(length);
        for (int anInt : ints) {
            System.out.println("i = " + anInt);
        }
    }

    private static void testUpdate(JDBCHelper jdbc) {
        int i = jdbc.executeUpdate("insert into user (id,name,age) values (?,?,?)", new Object[]{7, "邓丽辉", 32});
        System.out.println("i = " + i);
    }

    private static void testQuery(JDBCHelper jdbc) {
        final Map<String, Object> testquery = new HashMap<String, Object>();
        jdbc.executeQuery("select * from user where id = ?", new Object[]{3}, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
//                    String name = rs.getString("name");
//                    int age = rs.getInt("age");


                    //匿名内部类的使用，有一个很重要的知识点
                    //如果要访问外部类中的一些成员，比如方法内的局部变量
                    //那么，必须将局部变量，声明为final类型，才可以访问,否则是访问不了的

                    //数据结果集的角标是从1开始的。
                    String name = rs.getString(2);
                    int age = rs.getInt(3);
                    testquery.put("name", name);
                    testquery.put("age", age);

                }
            }
        });

        String name = (String) testquery.get("name");
        Integer age = (Integer) testquery.get("age");
        System.out.println(name);
        System.out.println(age);
    }

    private static JDBCHelper testJDBC() {
        JDBCHelper jdbc = JDBCHelper.getInstance();
        String str = "select * from user";
        Connection connection = jdbc.getConnection();
        try {
            Statement stat = connection.createStatement();
            ResultSet resultSet = stat.executeQuery(str);
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                System.out.println(name + "--" + age);

            }

            String insert_sql = " insert into user (id,name,age) values (6,'曹红丽',42)";
            stat.executeUpdate(insert_sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jdbc;
    }
}
