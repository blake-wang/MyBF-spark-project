
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wanglei on 2018/4/9.
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date time1 = sdf.parse("2018-01-01 12:00:00");
        Date time2 = sdf.parse("2018-01-01 12:00:01");
        boolean before = time1.before(time2);
        System.out.println(before);

    }
}
