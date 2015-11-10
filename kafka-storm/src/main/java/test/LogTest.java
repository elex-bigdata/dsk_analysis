package test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by wanghaixing on 15-11-10.
 */
public class LogTest {
    static Logger logger = LoggerFactory.getLogger(LogTest.class);


    public static void main(String[] args) throws Exception {
        while(true) {
            Thread.sleep(1000);
            logger.info("当前时间戳：" + System.currentTimeMillis());
        }
    }
}
