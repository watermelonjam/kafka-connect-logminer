/**
 * Copyright 2018 David Arnold
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.extr.kafka.connect.logminer.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

public class DatabaseTest {
	@Test
	public void testDatabaseConnection() throws Exception {
		Connection connection = null;
		try {
			String jdbcURL = "jdbc:oracle:thin:@pc-arnold-d4.mcmaster.ca:1521/DSAPOC.UTS.McMaster.CA";
			Properties properties = new Properties();
			properties.setProperty("user", "c##kminer");
			properties.setProperty("password", "kminer");
			connection = DriverManager.getConnection(jdbcURL, properties);
			
			DatabaseMetaData md = connection.getMetaData();
			System.out.println(md.getDatabaseProductName());
			System.out.println(md.getDatabaseProductVersion());
			PreparedStatement p = connection.prepareCall("select * from d");
			
			ResultSet rs = p.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
