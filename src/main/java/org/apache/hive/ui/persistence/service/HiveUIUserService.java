package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveUIUserEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

public class HiveUIUserService {

    public static HiveUIUserEntity findByUserName(String userName){
        Query query= HibernateUtils.getSession().createQuery("from HiveUIUserEntity where userName=:userName");
        query.setParameter("userName", userName);
        HiveUIUserEntity uveUIUserEntity = (HiveUIUserEntity) query.uniqueResult();
        return uveUIUserEntity;
    }

    public static Integer saveHiveUIUser(HiveUIUserEntity entity) {
        Session session = null;
        Integer id = null;
        try {
            session = HibernateUtils.getSession();
            session.beginTransaction();
            id = (Integer)session.save(entity);
            session.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
            session.getTransaction().rollback();
            return id;
        } finally {
            if (session != null) {
                session.close();
            }
            return id;
        }
    }
}
