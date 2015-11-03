package kudu.jdbc.hanlder;

import java.sql.ResultSet;
import java.sql.SQLException;

import kudu.jdbc.Hanlder;

/**
 * @author yanbit
 * @date Nov 3, 2015 5:37:07 PM
 * @todo TODO
 */
public class SingleResultHanlder implements Hanlder {
  public Object process(ResultSet rs) {
    Object count =null;
    try {
      while (rs.next()) {
        count = rs.getString(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    System.out.println("count:----------------"+count);
    return count;
  }
}
