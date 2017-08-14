package com.wyg.hadoop.mysql.db;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

import com.wyg.hadoop.mysql.bean.DBRecord;
import com.wyg.hadoop.mysql.mapper.WriteDB;

public class DBInsert {
	public static void main(String[] args) throws Exception {

		 

        JobConf conf = new JobConf(WriteDB.class);
        // ���������������

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(DBOutputFormat.class);

        // ���������䣬ͨ�������������ϸ�������û�������䡣
        //Text, DBRecord
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DBRecord.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DBRecord.class);
        // ����Map��Reduce��
        conf.setMapperClass(WriteDB.Map.class);
        conf.setReducerClass(WriteDB.Reduce.class);
        // ��������Ŀ¼
        FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.1.128:9000/temp/test.txt"));
        // �������ݿ�����
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver", "jdbc:mysql://192.168.1.106:3306/mldn","root","123456");
        String[] fields = {"id","title","content" };
        DBOutputFormat.setOutput(conf, "testHadoop", fields);
        JobClient.runJob(conf);
    }

}
