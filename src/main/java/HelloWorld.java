// mongodb download - https://www.mongodb.com/try/download/community?tck=docs_server
// https://www.mongodb.com/docs/drivers/java/sync/current/usage-examples/find/
// https://www.tutorialspoint.com/mongodb/mongodb_java.htm
// https://mkyong.com/mongodb/java-mongodb-query-document/

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Date;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

/**
 * Created by chokst on 3/2/15.
 */
public class HelloWorld {
    public static void main(String[] args) throws Exception {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        displayAllDbs(mongoClient);
        MongoDatabase db = getDbHandle(mongoClient);
        displayAllCollections(db);

        final MongoCollection<Document> mycollection = db.getCollection("mycollection");

        createDocument(mycollection);
        findDocument(mycollection);
        updateDocument(mycollection);
        findDocument(mycollection);
        dropDocument(mycollection);
        findDocument(mycollection);
    }

    private static void displayAllDbs(MongoClient mongoClient) {
        System.out.println("DBs:------");
        final MongoIterable<String> dbs = mongoClient.listDatabaseNames();
        for (String db : dbs) {
            System.out.println(db);
        }
    }

    private static MongoDatabase getDbHandle(MongoClient mongoClient) {
        //If MongoDB in secure mode, authentication is required.
        MongoDatabase db = mongoClient.getDatabase("mydb");
        //boolean auth = db.authenticate("username", "password".toCharArray());
        return db;
    }

    private static void displayAllCollections(MongoDatabase db) {
        System.out.println("Collections:-----");
        final MongoIterable<String> collectionNames = db.listCollectionNames();

        for (String coll : collectionNames) {
            System.out.println(coll);
        }
    }

    private static void createDocument(MongoCollection<Document> dbCollection) {
        BasicDBObject addressDoc = new BasicDBObject();
        addressDoc.put("city", "Sacramento");
        addressDoc.put("state", "California");

        Document document = new Document();
        document.put("name", "mkyong");
        document.put("age", 30);
        document.put("createdDate", new Date());
        document.put("address", addressDoc);


        dbCollection.insertOne(document);
    }

    private static void findDocument(MongoCollection<Document> dbCollection) {
        System.out.println("Found Documents:-----");

        MongoCursor<Document> cursor = dbCollection.find(eq("name", "mykong"))
                .sort(Sorts.descending("age")).iterator();

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }


    }

    private static void updateDocument(MongoCollection<Document> dbCollection) {
        System.out.println("Updating Document:-----");

        Bson query = gt("age", 10);
        Bson updates = Updates.combine(
                Updates.set("name", "mkyong-updated"),
                //Updates.addToSet("name", "mkyong-updated"),
                Updates.currentTimestamp("lastUpdated"));

        try {
            UpdateResult result = dbCollection.updateMany(query, updates);
            System.out.println("Modified document count: " + result.getModifiedCount());
        } catch (MongoException me) {
            System.err.println("Unable to update due to an error: " + me);
        }
    }

    private static void dropDocument(MongoCollection<Document> dbCollection) {
        System.out.println("Dropping Document:-----");

        Bson query = eq("address.city", "Sacramento");

        try {
            DeleteResult result = dbCollection.deleteMany(query);
            System.out.println("Deleted document count: " + result.getDeletedCount());
        } catch (MongoException me) {
            System.err.println("Unable to delete due to an error: " + me);
        }

    }
}
