package org.paf.hadoop.hadoopTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Testdb {
	
//    static String driver = "com.mysql.jdbc.Driver";  
////  static String url = "jdbc:mysql://192.168.1.58:3306/powerloaddata?user=dbuser&password=lfmysql";  
//    static String url = "jdbc:mysql://localhost:3306/mldn?user=root&password=123456";  
//    static Connection conn = null; 
//    static Statement stmt = null;
//    static ResultSet rs = null;
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
//        Class.forName(driver);  
//        conn = DriverManager.getConnection(url); 
//        stmt = conn.createStatement();
//        String sql = "insert into t values(1,'123')";
////        System.out.println(line.toString());
//        stmt.executeUpdate(sql);
		dbInsert();
	}
	public static void dbInsert() throws ClassNotFoundException, SQLException{
	     String driver = "com.mysql.jdbc.Driver";  
	//  static String url = "jdbc:mysql://192.168.1.58:3306/powerloaddata?user=dbuser&password=lfmysql";  
	     String url = "jdbc:mysql://localhost:3306/mldn?user=root&password=123456";  
	     Connection conn = null; 
	     Statement stmt = null;
	     ResultSet rs = null;
        Class.forName(driver);  
        conn = DriverManager.getConnection(url); 
        stmt = conn.createStatement();
        String sql = "insert into t values(1,'123')";
//        System.out.println(line.toString());
        stmt.executeUpdate(sql);
	}

}
