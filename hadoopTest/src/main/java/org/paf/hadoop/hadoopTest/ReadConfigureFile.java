package org.paf.hadoop.hadoopTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ReadConfigureFile {

	/**
	 * 
	 * @param args
	 * 
	 */

	public static void main(String[] args) {

		// TODO Auto-generated method stub

		String driver = "com.mysql.jdbc.Driver";

		String url = "jdbc:mysql://localhost:3306/mldn";

		String username = "root";

		String password = "123456";

		Connection conn = null;

		Statement stmt = null;

		File file = new File("D:\\tmp\\安徽联通.txt");

		StringBuffer sql = null;

		BufferedReader reader = null;

		String line = null;

		String[] str = null;

		int id = 0;

		String title = null;

		String content = null;

		try {

			Class.forName(driver);

			conn = DriverManager.getConnection(url, username, password);

			reader = new BufferedReader(new FileReader(file));

			stmt = conn.createStatement();

			while ((line = reader.readLine()) != null) {

				sql = new StringBuffer();

				str = line.split("-");
				if (str.length != 3){
					continue;
				}
				if(!str[0].trim().isEmpty()){
					id = Integer.parseInt(str[0].trim().toString());
//					id = str[0].trim().toString().length();
				}

				title = str[1];

				content = str[2];

				sql.append("insert into testhadoop(id,title,content) values('");

				sql.append(id + "','");

				sql.append(title + "','");

				sql.append(content + "')");

				stmt.executeUpdate(sql.toString());

			}

		} catch (FileNotFoundException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		} catch (IOException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		} catch (ClassNotFoundException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		} catch (SQLException e) {

			// TODO Auto-generated catch block

			e.printStackTrace();

		} finally {

			if (reader != null) {

				try {

					reader.close();

				} catch (IOException e) {

					// TODO Auto-generated catch block

					e.printStackTrace();

				}

			}

		}

	}

}

