package jm.task.core.jdbc.util;

import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.mysql.cj.jdbc.Driver;

public class Util {
    // реализуйте настройку соеденения с БД
    private static final Logger logger = Logger.getLogger(Util.class.getName());
    public static final String URL = "jdbc:mysql://localhost:3306/TestDataBase?serverTimezone=UTC";
    public static final String USER = "root";
    public static final String PASSWORD = "root";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            DriverManager.registerDriver(new Driver());
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            logger.info("DataBase Connection - OK");
        } catch (SQLException ex) {
            throw new RuntimeException("DataBase Connection - FAIL", ex);
        }
        return connection;
    }
}
