package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.jcraft.jsch.jce.MD5;

/**
 * @author yanbit
 * @date Nov 6, 2015 10:30:45 AM
 * @todo TODO
 */
public class MD5UDF extends UDF {

  private Text md5line = new Text();

  /**
   * gen md5 string
   * @param line
   * @return
   */
  public Text evaluate(Text line) {
    HashCode hashCode =
        Hashing.md5().hashString(line.toString(), Charsets.UTF_8);
    md5line.clear();
    md5line.set(hashCode.toString());
    return md5line;
  }
  
  
}
