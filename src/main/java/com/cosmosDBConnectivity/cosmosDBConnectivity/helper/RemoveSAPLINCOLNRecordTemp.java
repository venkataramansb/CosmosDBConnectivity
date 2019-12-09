package com.cosmosDBConnectivity.cosmosDBConnectivity.helper;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Iterator;

import static com.mongodb.client.model.Filters.eq;

public class RemoveSAPLINCOLNRecordTemp {
    public static void main(String ar[]) {
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI("mongodb://csdn-user-profile-cn-qa:nCjMgYe92SfRYOExKRncmwgyhzsuw7GXMdwIX20gSKMK74wGZ6Tr8eP5I4eNsA6EIpkuo8vei0teLXrU4B0ohw==@csdn-user-profile-cn-qa.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb"));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");


        FindIterable doc = sourceCollection.find(eq("userProfileStore", "SCA-CAP-CN"));

        Iterator iterator = doc.iterator();


        long insertCounter =0;
        long deleteCounter = 0;
        long totalCounter =0;
        while (iterator.hasNext()) {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", "SCACAPCN_" + document.get("lighthouseGuid").toString().toLowerCase())).first();

            if(obj != null) {
                /*insertCounter++;
                Document newDocument = transformNewDocument(document);
                newDocument.put("userProfileStore", "SCA-CAP-CN");
                newDocument.put("_id", "SCACAPCN_" + document.get("lighthouseGuid").toString().toLowerCase());

                System.out.println("########Inserting " + newDocument.get("_id"));
                sourceCollection.insertOne(newDocument);

                System.out.println("Inserted " + newDocument.get("_id"));

                System.out.println("Deleting " + document.get("_id"));
                //sourceCollection.deleteOne((Document) document);
                System.out.println("Deleted " + document.get("_id"));*/
           // } else {
                deleteCounter++;
                System.out.println(((Document)obj).get("_id") + "     #########Deleting " + document.get("_id"));
                sourceCollection.deleteOne((Document) document);
                System.out.println("Deleted " + document.get("_id"));
            }
            //System.out.println("------------------------------------>Inserted " + insertCounter);
            System.out.println("------------------------------------>Deleted  " + deleteCounter);
        }

        System.out.println("Total counter : " + totalCounter);
    }

    private static Document transformNewDocument(Document document) {
        Document newDocument = new Document();
        newDocument.put("_id", document.get("_id"));
        newDocument.put("_class", document.get("_class"));
        newDocument.put("lighthouseGuid", document.get("lighthouseGuid").toString().toLowerCase());
        newDocument.put("category", document.get("category"));
        newDocument.put("userProfileStore", "SCA-CAP-CN");
        newDocument.put("lastUpdatedDate", document.get("lastUpdatedDate"));
        newDocument.put("timezone", document.get("timezone"));
        newDocument.put("language", document.get("language"));
        newDocument.put("country", document.get("country"));
        newDocument.put("partition", document.get("partition"));
        newDocument.put("vin", document.get("vin"));
        newDocument.put("appId", document.get("appId"));
        newDocument.put("firstName", document.get("firstName"));
        return newDocument;
    }
}
