package test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.CharSet;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by user on 8/4/14.
 */
public class TestProducer {
  final static String TOPIC = "test_upusers";

  public static void main(String[] argv) {
    //sendUpUserMessage();

    String[] a = {"1","2","3"};
    System.out.println(Joiner.on(",").join(a));
  }

  /**
   *
   */
  public static void sendUpUserMessage() {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", "10.1.3.56:9092");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig producerConfig = new ProducerConfig(properties);
    kafka.javaapi.producer.Producer<String, String> producer =
        new kafka.javaapi.producer.Producer<String, String>(producerConfig);

    // ------------------------------------------------------------------------------------
    List<String> lines = null;
    try {
      lines = Files.readLines(new File(
              "/home/yanbit/Desktop/code/dsk_analysis/storm/src/main/resources/upusers.csv"),
          Charsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
    }
    int count = 0;
    for (String line : lines) {
      count++;
      if (count==100000){
        break;
      }
      ArrayList<String> list =
          new ArrayList<String>(Arrays.asList(line.split(",")));
      System.out.println(list);
      if (list.size() != 0) {
        String mid = list.remove(0);
        list.remove(0);
        String nline =
            UUID.randomUUID().toString() + "," + Joiner
                .on(",").join(list.toArray()).toString();
        KeyedMessage<String, String> message =
            new KeyedMessage<String, String>(TOPIC, nline);
        producer.send(message);

      }
    }
    try {
      System.out.println("start----------");
      TimeUnit.SECONDS.sleep(10);
      System.out.println("start----------");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    producer.close();

  }
}
