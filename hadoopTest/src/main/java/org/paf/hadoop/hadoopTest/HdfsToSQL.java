package org.paf.hadoop.hadoopTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;





public class HdfsToSQL {
    static String driver = "com.mysql.jdbc.Driver";  
//  static String url = "jdbc:mysql://192.168.1.58:3306/powerloaddata?user=dbuser&password=lfmysql";  
    static String url = "jdbc:mysql://localhost:3306/mldn?user=root&password=123456";  
    static Connection conn = null; 
    static Statement stmt = null;
    static ResultSet rs = null;

    public static class hdfsToSQLMapper extends Mapper<Object, Text, Text, IntWritable>{



        public void map(Object key , Text value, Context context) throws IOException, InterruptedException {
            // get lines
            String line = value.toString();
            String [] words = line.split(",");
            if (words.length == 3){

                try {
                    // write sql
                    Class.forName(driver);  
                    conn = DriverManager.getConnection(url); 
                    stmt = conn.createStatement();
                    String sql = "insert into t2 values("+words[0]+","+words[1]+")";

                    stmt.executeUpdate(sql);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {

                    try {
                        conn.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }


            }


        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

//        if (otherArgs.length != 2) {
//
//            System.err.println("Usage: wordcount <in> <out>");
//
//            System.exit(2);
//
//        }

        Job job = Job.getInstance(conf, "hdfsToSQL");

        job.setJarByClass(HdfsToSQL.class);

        job.setMapperClass(hdfsToSQLMapper.class);

//      job.setCombinerClass(IntSumReducer.class);

//      job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.128:9000/temp/cite2.txt"));

        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.128:9000/temp/tttt.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
