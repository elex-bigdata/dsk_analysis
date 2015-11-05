package kudu.jdbc;

import kudu.jdbc.hanlder.Hanlder;

/**
 * @author yanbit
 * @date Nov 5, 2015 10:29:46 AM
 * @todo TODO
 */
public interface Query<T> {
  public T query(String sql, Hanlder hanlder);
}
