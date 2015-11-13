package test;

/**
 * Created with IntelliJ IDEA.
 * User: yanbit
 * Date: 11/12/15
 * Time: 4:06 PM
 * Description:
 */
public class TestException {
  public static void main(String[] args) {
    try {
      hello();
    } catch (Exception e) {
      System.out.println(e);
      for(StackTraceElement elem : e.getStackTrace()) {
        System.out.println(elem);
      }
    }
  }

  public static void hello() throws Exception {
    throw new Exception();
  }
}
