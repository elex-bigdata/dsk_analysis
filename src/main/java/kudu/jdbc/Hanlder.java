package kudu.jdbc;

import java.sql.ResultSet;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:55:22 PM
 * @todo TODO
 */
public interface Hanlder {
  public Object process(ResultSet rs);
}
