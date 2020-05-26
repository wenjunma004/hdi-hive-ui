package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveIJobEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

import java.util.List;

public class HiveUIJobService {

    public static List<HiveIJobEntity> findJobsByUserId(Integer userID){
        Query query= HibernateUtils.getSession().createQuery("from HiveIJobEntity where userID=:userID");
        query.setParameter("userID", userID);
        List<HiveIJobEntity> entities = query.getResultList();
        return entities;
    }

    public static List<HiveIJobEntity> findJobsByGuiId(String guid){
        Query query= HibernateUtils.getSession().createQuery("from HiveIJobEntity where guid=:guid");
        query.setParameter("guid", guid);
        List<HiveIJobEntity> entities = query.getResultList();
        return entities;
    }


    public static HiveIJobEntity findJobById(Integer id){
        Query query= HibernateUtils.getSession().createQuery("from HiveIJobEntity where id=:id");
        query.setParameter("id", id);
        HiveIJobEntity entity = (HiveIJobEntity)query.getSingleResult();
        return entity;
    }

    public static boolean updateHiveUIJob(HiveIJobEntity entity) {
        Session session = null;
        try {
            session = HibernateUtils.getSession();
            System.out.println("update:" + entity);
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


    public static Integer saveHiveUIJob(HiveIJobEntity entity) {
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

    public static boolean deleteHiveUIJob(HiveIJobEntity entity) {
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
