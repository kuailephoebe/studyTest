package org.paf.hadoop.hadoopTest;
import java.net.URI;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class FileCopy2Local
{
 public static void main(String[] args) throws Exception
 {
  String dest = "hdfs://192.168.1.128:9000/temp/test.txt";
  String local = "D:/tmp/cite2.txt";
  Configuration conf = new Configuration();
  FileSystem fs = FileSystem.get(URI.create(dest),conf);
  FSDataInputStream fsdi = fs.open(new Path(dest));
  OutputStream output = new FileOutputStream(local);
  IOUtils.copyBytes(fsdi,output,4096,true);
 }
}