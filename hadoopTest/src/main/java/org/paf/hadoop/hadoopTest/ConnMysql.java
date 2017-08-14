package org.paf.hadoop.hadoopTest;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConnMysql {
        
        private static Configuration conf = new Configuration();
        
        static {
                conf.addResource(new Path("F:/lxw-hadoop/hdfs-site.xml"));
                conf.addResource(new Path("F:/lxw-hadoop/mapred-site.xml"));
                conf.addResource(new Path("F:/lxw-hadoop/core-site.xml"));
                conf.set("mapred.job.tracker", "10.133.103.21:50021");
        }
        
        public static class TblsRecord implements Writable, DBWritable {
                String id;
                String name;

                public TblsRecord() {

                }

//                @Override
                public void write(PreparedStatement statement) throws SQLException {
                        statement.setString(1, this.id);
                        statement.setString(2, this.name);
                }

//                @Override
                public void readFields(ResultSet resultSet) throws SQLException {
                        this.id = resultSet.getString(1);
                        this.name = resultSet.getString(2);
                }

//                @Override
                public void write(DataOutput out) throws IOException {
                        Text.writeString(out, this.id);
                        Text.writeString(out, this.name);
                }

                public void readFields(DataInput in) throws IOException {
                        this.id = Text.readString(in);
                        this.name = Text.readString(in);
                }

                public String toString() {
                        return new String(this.id + " " + this.name);
                }

        }

        public static class ConnMysqlMapper extends Mapper<LongWritable,TblsRecord,Text,Text> {
                public void map(LongWritable key,TblsRecord values,Context context) 
                                throws IOException,InterruptedException {
                        context.write(new Text(values.id), new Text(values.name));
                }
        }
        
        public static class ConnMysqlReducer extends Reducer<Text,Text,Text,Text> {
                public void reduce(Text key,Iterable<Text> values,Context context) 
                                throws IOException,InterruptedException {
                        for(Iterator<Text> itr = values.iterator();itr.hasNext();) {
                                context.write(key, itr.next());
                        }
                }
        }
        
        public static void main(String[] args) throws Exception {
                Path output = new Path("hdfs://192.168.1.128:9000/temp/t.txt");
                
                FileSystem fs = FileSystem.get(URI.create(output.toString()), conf);
                if (fs.exists(output)) {
                        fs.delete(output);
                }
                
                //mysql的jdbc驱动
                DistributedCache.addFileToClassPath(new Path(  
                          "/tmp/mysql-connector-java-5.1.38.jar"), conf);  
                
                DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",  
                          "jdbc:mysql://localhost:3306/mldn", "root", "123456");  
                
                Job job = new Job(conf,"test mysql connection");
                job.setJarByClass(ConnMysql.class);
                
                job.setMapperClass(ConnMysqlMapper.class);
                job.setReducerClass(ConnMysqlReducer.class);
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setInputFormatClass(DBInputFormat.class);
                FileOutputFormat.setOutputPath(job, output);
                
                //列名
                String[] fields = { "id", "name" }; 
                //六个参数分别为：
                //1.Job;2.Class<? extends DBWritable>
                //3.表名;4.where条件
                //5.order by语句;6.列名
                DBInputFormat.setInput(job, TblsRecord.class,
                     "t", "id like '*'", "name", fields);  
                
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        
}
