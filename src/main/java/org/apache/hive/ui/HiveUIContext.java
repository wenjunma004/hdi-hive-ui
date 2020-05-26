package org.apache.hive.ui;

public class HiveUIContext {

    public static String getUsername(){
            return "admin";
    }

    public static String getInstanceName(){
        return "Hive-UI";
    }

    public static String getTimelineServerUrl(){ return "http://10.160.254.62:8188" ;}

    public static String getHiveJDBCUrl(){
        //return "jdbc:hive2://10.160.254.62:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;hive.server2.proxy.user=admin";
        return "jdbc:hive2://10.160.254.62:10000";
    }


}
