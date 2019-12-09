package com.cosmosDBConnectivity.cosmosDBConnectivity.helper;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;

public class DuplicateMetadataSAPRecord {
    public static void main(String ar[]) {

        System.out.println( "Instant"+Instant.now().toString());
        MongoClient sourceMongoClient = null;
        sourceMongoClient = new MongoClient(new MongoClientURI("mongodb://csdn-user-profile-cn-stage:gL8cO4EIKQ3ff3rtXwYKGtAyI4WZfR79b7SDkbwtPuO2CvGEYU6OTLVeDSN9pstGJNo4VF3Zk7KP97IgxveQAQ==@csdn-user-profile-cn-stage.documents.azure.cn:10255/user-profile?ssl=true&replicaSet=globaldb"));

        MongoDatabase sourceDatabase = sourceMongoClient.getDatabase("user-profile");
        MongoCollection sourceCollection = sourceDatabase.getCollection("userProfileMetaData");

        long count1 = sourceCollection.count(lt("userProfileStore", "Lincoln CRM"));
        long count2 = sourceCollection.count(lt("userProfileStore", "Ford CRM"));
        long count3 = sourceCollection.count(lt("userProfileStore", "SCA-CAP-CN"));
        System.out.println("Count 1 --->"+ count1);
        System.out.println("Count 2 --->"+ count2);
        System.out.println("Count 3 --->"+ count3);
        sourceCollection.find(eq("",""));


        //FindIterable doc = sourceCollection.find(eq("userProfileStore", "Lincoln CRM"));
        FindIterable doc = sourceCollection.find(eq("lastUpdatedDate", "2018-05-10"));

        long count = sourceCollection.count();
        System.out.println("------- Store Name -- Lincoln CRM -- Count"  + count );

        Iterator iterator = doc.iterator();


        long insertCounter =0;
        long deleteCounter = 0;
        long totalCounter =0;
        while (iterator.hasNext()) {
            totalCounter++;
            Document document = (Document)iterator.next();
            System.out.println("-----Start----");
            System.out.println("----END-----");
            System.out.println(document.get("_id") + "---------->" + document.get("userProfileStore") + "---->" + totalCounter);
            Object obj = sourceCollection.find(eq("_id", "SCACAPCN_" + document.get("lighthouseGuid").toString().toLowerCase())).first();
            System.out.println(obj);
            List<Document> docList = new ArrayList<>();

            if(obj == null) {
                insertCounter++;
                Document newDocument = transformNewDocument(document);
                newDocument.put("userProfileStore", "SCA-CAP-CN");
                newDocument.put("_id", "SCACAPCN_" + document.get("lighthouseGuid").toString().toLowerCase());

                System.out.println("########Inserting " + newDocument.get("_id"));
                //sourceCollection.insertOne(newDocument);
                docList.add(newDocument);

                System.out.println("Inserted " + newDocument.get("_id"));

                System.out.println("Deleting " + document.get("_id"));
                //sourceCollection.deleteOne((Document) document);
                System.out.println("Deleted " + document.get("_id"));
            } /*else {
                deleteCounter++;
                System.out.println(((Document)obj).get("_id") + "     #########Deleting " + document.get("_id"));
                // sourceCollection.deleteOne((Document) document);
                System.out.println("Deleted " + document.get("_id"));
            }*/
            if(docList.size() !=0 && !docList.isEmpty()){

                sourceCollection.insertMany(docList);
            }
            System.out.println("------------------------------------>Inserted " + insertCounter);
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
