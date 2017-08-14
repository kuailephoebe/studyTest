//package org.paf.hadoop.hadoopTest;
//
//import java.io.IOException;
//import java.util.Iterator;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
//import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//
///**
// * @author
// * @version 创建时间：Jul 24, 2014 2:09:22 AM
// * 类说明
// */
//public class AccessData {
//
//    public static class DataAccessMap extends Mapper<LongWritable,YqBean,Text,Text>{
//        @Override
//        protected void map(LongWritable key, YqBean value,Context context)
//                throws IOException, InterruptedException {
//                System.out.println(value.toString());
//                context.write(new Text(), new Text(value.toString()));
//        }
//    }
//    
//    public static class DataAccessReducer extends Reducer<Text,Text,Text,Text>{
//        protected void reduce(Text key, Iterable<Text> values,
//                Context context)
//                throws IOException, InterruptedException {
//             for(Iterator<Text> itr = values.iterator();itr.hasNext();)   
//             {    
//                     context.write(key, itr.next());    
//             }   
//        }
//    }
//    public static void main(String[] args) throws Exception {
//    
//        Configuration conf = new Configuration();   
//        //mysql的jdbc驱动
//        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver", "jdbc:mysql://ip:3306/tablename?useUnicode=true&characterEncoding=utf8", "username", "passwd");    
//        Job job = new Job(conf,"test mysql connection");    
//        job.setJarByClass(AccessData.class);    
//            
//        job.setMapperClass(DataAccessMap.class);    
//        job.setReducerClass(DataAccessReducer.class);    
//            
//        job.setOutputKeyClass(Text.class);    
//        job.setOutputValueClass(Text.class);    
//            
//        job.setInputFormatClass(DBInputFormat.class); 
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://ip:9000/hdfsFile"));    
//            
//        //对应数据库中的列名(实体类字段)    
//       String[] fields = {"id","title","price","author","quantity","description","category_id","imgUrl"};  
//        DBInputFormat.setInput(job, YqBean.class,"tablename", "sql语句 ", "title", fields);   
//        System.exit(job.waitForCompletion(true)? 0 : 1);  
//
//  
//    }
//
//}
