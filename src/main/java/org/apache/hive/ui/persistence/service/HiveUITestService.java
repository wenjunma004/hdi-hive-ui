package org.apache.hive.ui.persistence.service;

import org.apache.hive.ui.persistence.po.HiveUITestEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;
import org.hibernate.Session;


public class HiveUITestService {
    public static boolean test(HiveUITestEntity entity) {
        Session session = null;
        try {
            session = HibernateUtils.getSession();
            session.beginTransaction();
            session.save(entity);
            session.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
            session.getTransaction().rollback();
            return false;
        } finally {
            if (session != null) {
                session.close();
            }
            return true;
        }
    }
}
