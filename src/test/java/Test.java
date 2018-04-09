import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

/**
 * Created by wanglei on 2018/4/9.
 */
public class Test {
    public static void main(String[] args) {
        String property = ConfigurationManager.getProperty("jdbc.driver");
        System.out.println(property);


        System.out.println(ConfigurationManager.getProperty(Constants.JDNC_DRIVER));
    }
}
