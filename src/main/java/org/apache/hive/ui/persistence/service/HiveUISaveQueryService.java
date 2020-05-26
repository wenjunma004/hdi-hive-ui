package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveUISaveQueryEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

import java.util.List;

public class HiveUISaveQueryService {

    public static List<HiveUISaveQueryEntity> findSaveQueriesByUserId(Integer userID){
        Query query= HibernateUtils.getSession().createQuery("from HiveUISaveQueryEntity where userID=:userID");
        query.setParameter("userID", userID);
        List<HiveUISaveQueryEntity> entities = query.getResultList();
        return entities;
    }

    public static HiveUISaveQueryEntity findSaveQueryById(Integer id){
        Query query= HibernateUtils.getSession().createQuery("from HiveUISaveQueryEntity where id=:id");
        query.setParameter("id", id);
        HiveUISaveQueryEntity entity = (HiveUISaveQueryEntity)query.getSingleResult();
        return entity;
    }

    public static boolean updateHiveUISaveQuery(HiveUISaveQueryEntity entity) {
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


    public static Integer saveHiveUISaveQuery(HiveUISaveQueryEntity entity) {
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

    public static boolean deleteHiveUISaveQuery(HiveUISaveQueryEntity entity) {
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
