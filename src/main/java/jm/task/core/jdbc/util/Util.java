package jm.task.core.jdbc.util;

import jm.task.core.jdbc.model.User;

import java.util.logging.Logger;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import com.mysql.cj.jdbc.Driver;

public class Util {
    // реализуйте настройку соеденения с БД
    private static final Logger logger = Logger.getLogger(Util.class.getName());
    public static final String URL = "jdbc:mysql://localhost:3306/TestDataBase?serverTimezone=UTC";
    public static final String USER = "root";
    public static final String PASSWORD = "root";
    public static final String DIALECT = "org.hibernate.dialect.MySQLDialect";
    public static final String HBM2DDL = "update";
    public static final String SHOW_SQL = "true";
    public static final String CONNECTION_POOL_TIMEOUT = "0";

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

    public static SessionFactory getSessionFactory() {
        Properties properties = new Properties();
        properties.setProperty(Environment.DRIVER, Driver.class.getCanonicalName());
        properties.setProperty(Environment.URL, URL);
        properties.setProperty(Environment.USER, USER);
        properties.setProperty(Environment.PASS, PASSWORD);
        properties.setProperty(Environment.DIALECT, DIALECT);
        properties.setProperty(Environment.HBM2DDL_AUTO, HBM2DDL);
        properties.setProperty(Environment.SHOW_SQL, SHOW_SQL);
        properties.setProperty(Environment.C3P0_TIMEOUT, CONNECTION_POOL_TIMEOUT);

        Configuration configuration = new Configuration();
        configuration.setProperties(properties);
        configuration.addAnnotatedClass(User.class);

        StandardServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
        SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
        logger.info("Get session factory - OK");
        return sessionFactory;
    }
}
