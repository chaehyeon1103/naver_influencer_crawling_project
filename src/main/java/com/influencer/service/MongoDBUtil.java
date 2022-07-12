package com.influencer.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDBUtil {
    private static final String HOST = "@61.97.191.52";
    private static final String PORT = "27017";
    private static final String DB = "influencer";
    private static final String USERNAME = "curadar_influencer";
    private static final String PASSWORD = "ckdl070%25";
    static String uri = "mongodb://"+USERNAME+ ":"+ PASSWORD +
            HOST+":"+PORT+"/?authSource=" + DB +
            "&readPreference=primary&appname=MongoDB%20Compass&ssl=false" +
            "&maxPoolSize=20&w=majority";

//    static String uri = "mongodb://"+USERNAME+ ":"+ PASSWORD +
//            HOST+":"+PORT+"/?" +
//            "readPreference=primary&appname=MongoDB%20Compass&ssl=false" +
//            "&maxPoolSize=20&w=majority";

    public static MongoClient createMongo = MongoClients.create(uri);
}
