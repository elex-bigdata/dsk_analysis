import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: yanbit
 * Date: 11/11/15
 * Time: 4:00 PM
 * Description:
 */
public class TestCache {
  public static void main(String[] args) throws Exception{
//    LoadingCache<String,String> cahceBuilder= CacheBuilder
//        .newBuilder()
//        .build(new CacheLoader<String, String>(){
//          @Override
//          public String load(String key) throws Exception {
//            //String strProValue="hello "+key+"!";
//            String strProValue="-----------------";
//            return strProValue;
//          }
//
//        });
//    System.out.println("jerry value:"+cahceBuilder.apply("jerry"));
//    System.out.println("jerry value:"+cahceBuilder.get("jerry"));
//    System.out.println("peida value:"+cahceBuilder.get("peida"));
//    System.out.println("peida value:"+cahceBuilder.apply("peida"));
//    System.out.println("lisa value:"+cahceBuilder.apply("lisa"));
//    cahceBuilder.put("harry", "ssdded");
//    System.out.println("harry value:"+cahceBuilder.get("harry"));

    Cache<String, String> cache = CacheBuilder.newBuilder().maximumSize(1000).build();
    String resultVal = cache.get("jerry", new Callable<String>() {
      public String call() {
        String strProValue="hello "+"jerry"+"!";
        return strProValue;
      }
    });
    System.out.println("jerry value : " + resultVal);

    resultVal = cache.get("peida", new Callable<String>() {
      public String call() {
        String strProValue="hello "+"peida"+"!";
        return strProValue;
      }
    });
    System.out.println("peida value : " + resultVal);
  }
}
