package org.apache.hive.ui.persistence.test;

import org.apache.hive.ui.persistence.po.*;
import org.apache.hive.ui.persistence.service.*;

public class Main {

    public static void main(String[] args) {

//        -- User Test
//        HiveUIUserEntity entity = new HiveUIUserEntity();
//        entity.setUserName("admin");
//        HiveUIUserService.saveHiveUIUser(entity);
//        System.out.println(HiveUIUserService.findByUserName("admin"));

//        -- System test
//        HiveUITestEntity test = new HiveUITestEntity();
//        test.setContent("test");
//        System.out.println(HiveUITestService.test(test));


//        -- HiveUIFileSourceItemEntity Test
////        HiveUIFileSourceItemEntity itemEntry = new HiveUIFileSourceItemEntity();
////        itemEntry.setUserID(1);
////        itemEntry.setName("cc");
////        itemEntry.setOwner("admin");
////        itemEntry.setPath("fdf");
//
//        //HiveUIFileSourceItemService.saveHiveUIFileSourceItem(itemEntry);
//       // System.out.println(HiveUIFileSourceItemService.findFileSourceItemsByUserId(1));
////        System.out.println(HiveUIFileSourceItemService.findFileSourceItemById(1));
////        HiveUIFileSourceItemEntity itemEntry2 = HiveUIFileSourceItemService.findFileSourceItemById(1);
////        itemEntry2.setName("updated");
////        System.out.println(HiveUIFileSourceItemService.updateHiveUIFileSourceItem(itemEntry2));
////        System.out.println(HiveUIFileSourceItemService.deleteHiveUIFileSourceItem(itemEntry2));

//        -- UDF Test
//        HiveUIUDFEntity hiveUIUDFEntity = new HiveUIUDFEntity();
//        hiveUIUDFEntity.setClassname("class name2");
//        hiveUIUDFEntity.setFileResource("resouce2");
//        hiveUIUDFEntity.setName("name2");
//        hiveUIUDFEntity.setOwner("admin");
//        hiveUIUDFEntity.setUserID(1);
//        HiveUIUDFService.saveHiveUIUDF(hiveUIUDFEntity);
//        System.out.println(HiveUIUDFService.findUDFsByUserId(1));
//        HiveUIUDFEntity hiveUIUDFEntity2= HiveUIUDFService.findUDFById(1);
//        System.out.println(hiveUIUDFEntity2);
//        hiveUIUDFEntity2.setName("updated");
//        HiveUIUDFService.updateHiveUIUDF(hiveUIUDFEntity2);
//        System.out.println(HiveUIUDFService.findUDFsByUserId(1));
//        System.out.println(HiveUIUDFService.deleteHiveUIUDF(HiveUIUDFService.findUDFById(1)));

//        -- SettingEntity test
//        HiveUISettingEntity hiveUISettingEntity = new HiveUISettingEntity();
//        hiveUISettingEntity.setKey("key");
//        hiveUISettingEntity.setOwner("admin");
//        hiveUISettingEntity.setValue("false");
//        hiveUISettingEntity.setUserID(1);
//
//        //HiveUISettingService.saveHiveUISetting(hiveUISettingEntity);
//        System.out.println(HiveUISettingService.findSettingsByUserId(1));
//        HiveUISettingEntity hiveUISettingEntity2 = HiveUISettingService.findSettingById(3);
//        System.out.println(hiveUISettingEntity2);
//        hiveUISettingEntity2.setValue("updated");
//        System.out.println(HiveUISettingService.updateHiveUISetting(hiveUISettingEntity2));
//        System.out.println(HiveUISettingService.findSettingById(3));
//        HiveUISettingService.deleteHiveUISetting(HiveUISettingService.findSettingById(3));
//        System.out.println("after delete");
//        System.out.println(HiveUISettingService.findSettingById(3));

        // -- test HiveUISaveQueryEntity
//        HiveUISaveQueryEntity  hiveUISaveQueryEntity = new HiveUISaveQueryEntity();
//        hiveUISaveQueryEntity.setDataBase("database");
//        hiveUISaveQueryEntity.setOwner("admin");
//        hiveUISaveQueryEntity.setQueryFile("queryfile");
//        hiveUISaveQueryEntity.setShortQuery("shortquery");
//        hiveUISaveQueryEntity.setTitle("title");
//        hiveUISaveQueryEntity.setUserID(1);
//        HiveUISaveQueryService.saveHiveUISaveQuery(hiveUISaveQueryEntity);
//        System.out.println(HiveUISaveQueryService.findSaveQueriesByUserId(1));
//        HiveUISaveQueryEntity hiveUISaveQueryEntity1 = HiveUISaveQueryService.findSaveQueryById(1);
//        System.out.println(hiveUISaveQueryEntity1);
//        hiveUISaveQueryEntity1.setTitle("updated");
//        System.out.println(HiveUISaveQueryService.updateHiveUISaveQuery(hiveUISaveQueryEntity1));
//        System.out.println(HiveUISaveQueryService.findSaveQueryById(1));
//        System.out.println(HiveUISaveQueryService.deleteHiveUISaveQuery(HiveUISaveQueryService.findSaveQueryById(1)));
//        System.out.println(HiveUISaveQueryService.findSaveQueryById(1));

//
        HiveIJobEntity hiveIJobEntity = new HiveIJobEntity();
        hiveIJobEntity.setApplicationId("id");
        hiveIJobEntity.setConfFile("confifle");
        hiveIJobEntity.setDagId("dagid");
        hiveIJobEntity.setDagName("dagname");
        hiveIJobEntity.setDataBase("db");
        hiveIJobEntity.setDateSubmitted(12);
        hiveIJobEntity.setDuration(2);
        hiveIJobEntity.setForcedContent("fourcedContent");
        hiveIJobEntity.setGlobalSettings("global");
        hiveIJobEntity.setGuid("guid");
        hiveIJobEntity.setHiveQueryId("queyid");
        hiveIJobEntity.setJobID(101);
        hiveIJobEntity.setLogFile("logfile");
        hiveIJobEntity.setOwner("admin");
        hiveIJobEntity.setReferrer("referrer");
        hiveIJobEntity.setSessionTag("sessionTag");
        hiveIJobEntity.setSqlState("sqlstate");
        hiveIJobEntity.setStatus("state");
        hiveIJobEntity.setStatusMessage("message");
        hiveIJobEntity.setStatusDir("date dir");
        hiveIJobEntity.setTitle("title");
        hiveIJobEntity.setUserID(1);
//        HiveUIJobService.saveHiveUIJob(hiveIJobEntity);
//        HiveUIJobService.saveHiveUIJob(hiveIJobEntity);
        System.out.println(HiveUIJobService.findJobsByUserId(1));
        HiveIJobEntity hiveIJobEntity2 = HiveUIJobService.findJobById(1);
        hiveIJobEntity2.setApplicationId("updated");
        System.out.println(HiveUIJobService.updateHiveUIJob(hiveIJobEntity2));
        System.out.println(HiveUIJobService.findJobById(1));
        System.out.println(HiveUIJobService.deleteHiveUIJob(HiveUIJobService.findJobById(1)));



    }
}
