package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveUISettingEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

import java.util.List;

public class HiveUISettingService {

    public static List<HiveUISettingEntity> findSettingsByUserId(Integer userID){
        Query query= HibernateUtils.getSession().createQuery("from HiveUISettingEntity where userID=:userID");
        query.setParameter("userID", userID);
        List<HiveUISettingEntity> entities = query.getResultList();
        return entities;
    }

    public static HiveUISettingEntity findSettingById(Integer id){
        Query query= HibernateUtils.getSession().createQuery("from HiveUISettingEntity where id=:id");
        query.setParameter("id", id);
        HiveUISettingEntity entity = (HiveUISettingEntity)query.getSingleResult();
        return entity;
    }

    public static boolean updateHiveUISetting(HiveUISettingEntity entity) {
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


    public static Integer saveHiveUISetting(HiveUISettingEntity entity) {
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

    public static boolean deleteHiveUISetting(HiveUISettingEntity entity) {
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
