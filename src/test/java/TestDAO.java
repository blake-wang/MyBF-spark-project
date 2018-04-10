import com.bf.sparkproject.dao.ITaskDAO;
import com.bf.sparkproject.dao.impl.DAOFactory;
import com.bf.sparkproject.domain.Task;

/**
 * Created by wanglei on 2018/4/9.
 */
public class TestDAO {
    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task byId = taskDAO.findById(1);
        System.out.println(byId.getTask_name());
    }
}
