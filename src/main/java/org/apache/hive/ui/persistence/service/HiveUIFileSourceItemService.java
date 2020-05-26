package org.apache.hive.ui.persistence.service;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.apache.hive.ui.persistence.po.HiveUIFileSourceItemEntity;
import org.apache.hive.ui.persistence.util.HibernateUtils;

import java.util.List;

public class HiveUIFileSourceItemService {

    public static List<HiveUIFileSourceItemEntity> findFileSourceItemsByUserId(Integer userID){
        Query query= HibernateUtils.getSession().createQuery("from HiveUIFileSourceItemEntity where userID=:userID");
        query.setParameter("userID", userID);
        List<HiveUIFileSourceItemEntity> entities = query.getResultList();
        return entities;
    }

    public static HiveUIFileSourceItemEntity findFileSourceItemById(Integer id){
        Query query= HibernateUtils.getSession().createQuery("from HiveUIFileSourceItemEntity where id=:id");
        query.setParameter("id", id);
        HiveUIFileSourceItemEntity entity = (HiveUIFileSourceItemEntity)query.getSingleResult();
        return entity;
    }

    public static boolean updateHiveUIFileSourceItem(HiveUIFileSourceItemEntity entity) {
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


    public static Integer saveHiveUIFileSourceItem(HiveUIFileSourceItemEntity entity) {
        Session session = null;
        Integer id  = null;
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

    public static boolean deleteHiveUIFileSourceItem(HiveUIFileSourceItemEntity entity) {
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
