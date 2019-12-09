package com.cosmosDBConnectivity.cosmosDBConnectivity.controller;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

@RestController
@RequestMapping(value = "/api/scacap", produces = MediaType.APPLICATION_JSON_VALUE)
public class SCACAPController
{

    static Map<String, String> connectionStringMap = new HashMap<>(5);
    static Map<String, String> userProfileStoreIdMap = new HashMap<>(5);


    SCACAPController() {
        connectionStringMap.put("qa", "mongodb://csdn-user-profile-cn-qa:nCjMgYe92SfRYOExKRncmwgyhzsuw7GXMdwIX20gSKMK74wGZ6Tr8eP5I4eNsA6EIpkuo8vei0teLXrU4B0ohw==@csdn-user-profile-cn-qa.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb");
        connectionStringMap.put("stage", "mongodb://csdn-user-profile-cn-stage:gL8cO4EIKQ3ff3rtXwYKGtAyI4WZfR79b7SDkbwtPuO2CvGEYU6OTLVeDSN9pstGJNo4VF3Zk7KP97IgxveQAQ==@csdn-user-profile-cn-stage.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb");
        connectionStringMap.put("perf", "mongodb://csdn-user-profile-cn-perf:iBlH3pLQawF4GJoljjnwLhUH3fvIxlLqzN562tSKnncIDWtQBmgT6JQPlKCg02Rw2dOUjvo4kyr8vl9M4Fq6CQ==@csdn-user-profile-cn-perf.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb");
        connectionStringMap.put("prod", "mongodb://csdn-user-profile-cn-prod:G05s98jhBJDn751wQ8D2B3YwMaJbcOJc4dMxgU2vWcXj2pJqaqOxPVVK8MxDSb0rxdQE9ZHZdfCRZfTC061bQw==@csdn-user-profile-cn-prod.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb");

        userProfileStoreIdMap.put("Ford CRM","SAPCRM_");
        userProfileStoreIdMap.put("Lincoln CRM","SAPLINCOLN_");
        userProfileStoreIdMap.put("SCA-CAP-CN","SCACAPCN_");
    }


    @RequestMapping(value = "/duplicateData",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)

    public ResponseEntity<String> duplicateCAPRecord( @RequestHeader("env") String env,
                                                      @RequestHeader("profileStore") String profileStore)
    {
        long lStartTime = System.nanoTime();
        List<Document> docList = new ArrayList();
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI(connectionStringMap.get(env.toLowerCase())));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");
        FindIterable doc = sourceCollection.find(eq("userProfileStore", profileStore));

        Iterator iterator = doc.iterator();
        long insertCounter =0;
        long totalCounter =0;

        while (iterator.hasNext()) {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase())).first();

            if(obj == null) {
                insertCounter++;
                Document newDocument = transformNewDocument(document);
                newDocument.put("userProfileStore", "SCA-CAP-CN");
                newDocument.put("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase());

               // System.out.println("########Inserting " + newDocument.get("_id"));
               // sourceCollection.insertOne(newDocument);
                docList.add(newDocument);
               // System.out.println("Inserted " + newDocument.get("_id"));


            }

            System.out.println("------------------------------------>Document To be Inserted " + insertCounter);
           }
        long interMediateTime = System.nanoTime();
        System.out.println("interMediateTime");
        System.out.println(interMediateTime - lStartTime);

if(!docList.isEmpty() && docList.size() !=0){
    System.out.println("Insdide LIst" +docList.size());

    sourceCollection.insertMany(docList);
}
        long lEndTime = System.nanoTime();
        long output = lEndTime - lStartTime;
        System.out.println("Time : " + output );
        System.out.println("Total counter : " + totalCounter);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(value = "/duplicateSingleData",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)

    public ResponseEntity<String> duplicateSingleCAPRecord( @RequestHeader("env") String env,
                                                      @RequestHeader("profileStore") String profileStore)
    {
        long lStartTime = System.nanoTime();
     //   List<Document> docList = new ArrayList();
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI(connectionStringMap.get(env.toLowerCase())));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");
        FindIterable doc = sourceCollection.find(eq("userProfileStore", profileStore));

        Iterator iterator = doc.iterator();
        long insertCounter =0;
        long totalCounter =0;

        while (iterator.hasNext()) {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase())).first();

            if(obj == null) {
                insertCounter++;
                Document newDocument = transformNewDocument(document);
                newDocument.put("userProfileStore", "SCA-CAP-CN");
                newDocument.put("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase());

                 System.out.println("########Inserting " + newDocument.get("_id"));
                 sourceCollection.insertOne(newDocument);
               // docList.add(newDocument);
                // System.out.println("Inserted " + newDocument.get("_id"));


            }

            System.out.println("------------------------------------>Document Inserted " + insertCounter);
        }
        long interMediateTime = System.nanoTime();
        System.out.println("interMediateTime");
        System.out.println(interMediateTime - lStartTime);


        long lEndTime = System.nanoTime();
        long output = lEndTime - lStartTime;
        System.out.println("Time : " + output );
        System.out.println("Total counter : " + totalCounter);
        return ResponseEntity.ok().build();
    }



    @RequestMapping(value = "/deleteData",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE)

    public void deleteRecord( @RequestHeader("env") String env, @RequestHeader("profileStore") String profileStore)
    {

        long lStartTime = System.nanoTime();
        List<Document> docList = new ArrayList();
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI(connectionStringMap.get(env.toLowerCase())));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");
        FindIterable doc = sourceCollection.find(eq("userProfileStore", profileStore));

        Iterator iterator = doc.iterator();

        long deleteCounter = 0;
        long totalCounter =0;
        while (iterator.hasNext())
        {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", userProfileStoreIdMap.get(profileStore) + document.get("lighthouseGuid").toString().toLowerCase())).first();

            if(obj != null)
                {
                    deleteCounter++;
                    System.out.println(((Document)obj).get("_id") + "     #########Deleting " + document.get("_id"));
                    //sourceCollection.deleteOne((Document) document);
                    docList.add(document);
                    System.out.println("Deleted " + document.get("_id"));
                }
                System.out.println("------------------------------------>Document To be Deleted  " + deleteCounter);

        }
        long interMediateTime = System.nanoTime();
        System.out.println("interMediateTime");
        System.out.println(interMediateTime - lStartTime);

        if(!docList.isEmpty() && docList.size() !=0){
            System.out.println("Insdide LIst" +docList.size());

            sourceCollection.deleteMany((Document)docList);
        }
        long lEndTime = System.nanoTime();
        long output = lEndTime - lStartTime;
        System.out.println("Time : " + output );
        System.out.println("Total counter : " + totalCounter);
    }

    @RequestMapping(value = "/deleteSingleData",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE)

    public void deleteSingleRecord( @RequestHeader("env") String env, @RequestHeader("profileStore") String profileStore)
    {
        long lStartTime = System.nanoTime();
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI(connectionStringMap.get(env.toLowerCase())));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");
        FindIterable doc = sourceCollection.find(eq("userProfileStore", profileStore));

        Iterator iterator = doc.iterator();

        long deleteCounter = 0;
        long totalCounter =0;
        while (iterator.hasNext())
        {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", userProfileStoreIdMap.get(profileStore) + document.get("lighthouseGuid").toString().toLowerCase())).first();

            if(obj != null)
            {
                deleteCounter++;
                System.out.println(((Document)obj).get("_id") + "     #########Deleting " + document.get("_id"));
                sourceCollection.deleteOne((Document) document);
                System.out.println("Deleted " + document.get("_id"));
            }
            System.out.println("------------------------------------>Document Deleted  " + deleteCounter);

        }
        long interMediateTime = System.nanoTime();
        System.out.println("interMediateTime");
        System.out.println(interMediateTime - lStartTime);


        long lEndTime = System.nanoTime();
        long output = lEndTime - lStartTime;
        System.out.println("Time : " + output );
        System.out.println("Total counter : " + totalCounter);
    }

    @RequestMapping(value = "/InsertDeltaData",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)

    public ResponseEntity<String> insertDeltaCAPRecord( @RequestHeader("env") String env,
                                                        @RequestHeader("lastUpdatedDate") String deltaDate) {
        long lStartTime = System.nanoTime();
        List<Document> docList = new ArrayList();
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI(connectionStringMap.get(env.toLowerCase())));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");
        String lastUpdatedDate = "^" + deltaDate + ".*$";
        System.out.println("Last Updated Date - Regular expression" + lastUpdatedDate);

        BasicDBObject query = new BasicDBObject();
        query.put("lastUpdatedDate", Pattern.compile(lastUpdatedDate, Pattern.CASE_INSENSITIVE));
        FindIterable doc = sourceCollection.find(eq(query));

        Iterator iterator = doc.iterator();
        long insertCounter = 0;
        long totalCounter = 0;

        while (iterator.hasNext()) {
            totalCounter++;
            Document document = (Document) iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            //Object obj = sourceCollection.find(eq("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase())).first();

            /*if (obj == null) {
                insertCounter++;
                Document newDocument = transformNewDocument(document);
                newDocument.put("userProfileStore", "SCA-CAP-CN");
                newDocument.put("_id", userProfileStoreIdMap.get("SCA-CAP-CN") + document.get("lighthouseGuid").toString().toLowerCase());

                System.out.println("########Inserting " + newDocument.get("_id"));
                sourceCollection.insertOne(newDocument);
                docList.add(newDocument);
                System.out.println("Inserted " + newDocument.get("_id"));


            }
*/
        }

        long lEndTime = System.nanoTime();
        long output = lEndTime - lStartTime;
        System.out.println("Time : " + output);
        System.out.println("Total counter : " + totalCounter);
        return ResponseEntity.ok().build();
    }

    private static Document transformNewDocument(Document document) {
        Document newDocument = new Document();
        newDocument.put("_id", document.get("_id"));
        newDocument.put("partition", document.get("partition"));
        newDocument.put("lighthouseGuid", document.get("lighthouseGuid").toString().toLowerCase());
        newDocument.put("vin", document.get("vin"));
        newDocument.put("category", document.get("category"));
        newDocument.put("userProfileStore", "SCA-CAP-CN");
        newDocument.put("appId", document.get("appId"));
        newDocument.put("lastUpdatedDate", document.get("lastUpdatedDate"));
        newDocument.put("timezone", document.get("timezone"));
        newDocument.put("language", document.get("language"));
        newDocument.put("country", document.get("country"));
        newDocument.put("firstName", document.get("firstName"));
        newDocument.put("lastName", document.get("lastName"));
        newDocument.put("email", document.get("email"));
        newDocument.put("companyName", document.get("companyName"));
        newDocument.put("postalCode", document.get("postalCode"));
        newDocument.put("baiduOpenId", document.get("baiduOpenId"));

        return newDocument;
    }
}
