package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveUIUDFEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

import java.util.List;

public class HiveUIUDFService {

    public static List<HiveUIUDFEntity> findUDFsByUserId(Integer userID){
        Query query= HibernateUtils.getSession().createQuery("from HiveUIUDFEntity where userID=:userID");
        query.setParameter("userID", userID);
        List<HiveUIUDFEntity> entities = query.getResultList();
        return entities;
    }

    public static HiveUIUDFEntity findUDFById(Integer id){
        Query query= HibernateUtils.getSession().createQuery("from HiveUIUDFEntity where id=:id");
        query.setParameter("id", id);
        HiveUIUDFEntity entity = (HiveUIUDFEntity)query.getSingleResult();
        return entity;
    }

    public static boolean updateHiveUIUDF(HiveUIUDFEntity entity) {
        Session session = null;
        try {
            session = HibernateUtils.getSession();
            session.beginTransaction();
            session.update(entity);
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


    public static Integer saveHiveUIUDF(HiveUIUDFEntity entity) {
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

    public static boolean deleteHiveUIUDF(HiveUIUDFEntity entity) {
        Session session = null;
        try {
            session = HibernateUtils.getSession();
            session.beginTransaction();
            session.delete(entity);
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
