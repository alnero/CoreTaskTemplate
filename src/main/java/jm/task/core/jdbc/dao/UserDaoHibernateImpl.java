package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.util.Util;
import jm.task.core.jdbc.model.User;

import java.util.List;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class UserDaoHibernateImpl implements UserDao {
    public UserDaoHibernateImpl() {

    }

    @Override
    public void createUsersTable() {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        String sql = "CREATE TABLE IF NOT EXISTS `User` (" +
                "`id` BIGINT AUTO_INCREMENT NOT NULL," +
                "`name` VARCHAR(45) NULL," +
                "`lastName` VARCHAR(45) NULL," +
                "`age` TINYINT NOT NULL," +
                "PRIMARY KEY (`ID`));";
        session.createSQLQuery(sql).executeUpdate();
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
    }

    @Override
    public void dropUsersTable() {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        String sql = "DROP TABLE IF EXISTS `User`;";
        session.createSQLQuery(sql).executeUpdate();
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        User user = new User(name, lastName, age);
        session.save(user);
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
    }

    @Override
    public void removeUserById(long id) {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        session.delete(session.get(User.class, id));
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
    }

    @Override
    public List<User> getAllUsers() {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<User> result = session.createCriteria(User.class).list();
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
        return result;
    }

    @Override
    public void cleanUsersTable() {
        SessionFactory sessionFactory = Util.getSessionFactory();
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        String sql = "TRUNCATE TABLE `User`;";
        session.createSQLQuery(sql).executeUpdate();
        session.getTransaction().commit();
        session.close();
        sessionFactory.close();
    }
}
