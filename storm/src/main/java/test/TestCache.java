package test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Created with IntelliJ IDEA.
 * User: yanbit
 * Date: 11/13/15
 * Time: 9:54 AM
 * Description:
 */
public class TestCache {
  LoadingCache cache = CacheBuilder.newBuilder().maximumSize(100000).build(new CacheLoader() {
    @Override public Object load(Object o) throws Exception {
      return null;
    }
  });


}
