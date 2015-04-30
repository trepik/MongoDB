
import com.google.gson.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.util.*;
import java.util.*;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author tomm
 */
public class MongoDB {

    private final ServerAddress server;
    private MongoCredential cred;
    private MongoClient client;
    private DB db;
    private DBCollection coll;
    private DBCursor cur;

    MongoDB(String address, int port) {
        server = new ServerAddress(address, port);
    }

    boolean Authenticate(String uName, char[] pass) {
        cred = MongoCredential.createCredential(uName, "admin", pass);
        client = new MongoClient(server, Arrays.asList(cred));
        return !client.isLocked();
    }

    Object[] getDBs() {
        MongoCursor<String> iter = client.listDatabaseNames().iterator();
        ArrayList<String> dbs = new ArrayList<>();
        while (iter.hasNext()) {
            dbs.add(iter.next());
        }
        return dbs.toArray();
    }

    boolean insertDoc(String dbName, String colName, String data) {
        db = client.getDB(dbName);
        coll = db.getCollection(colName);
        DBObject doc;
        try {
            doc = (DBObject) JSON.parse(data);
        } catch (JSONParseException e) {
            return false;
        }
        return (null != coll.insert(doc, WriteConcern.ACKNOWLEDGED));
    }

    String getOne(String dbName, String colName) {
        db = client.getDB(dbName);
        coll = db.getCollection(colName);
        return prettyJSON(coll.findOne().toString());
    }

    String getAll(String dbName, String colName) {
        db = client.getDB(dbName);
        coll = db.getCollection(colName);
        cur = coll.find();
        String data = "";
        try {
            while (cur.hasNext()) {
                data = data.concat(prettyJSON(cur.next().toString())+",\n");
            }
        } finally {
            cur.close();
        }
        return data;
    }

    int insertMultiple(String dbName, String colName, String docs) {
        db = client.getDB(dbName);
        coll = db.getCollection(colName);
        DBObject doc;
        int i = 0;
        try {
            doc = (DBObject) JSON.parse(docs);
        } catch (JSONParseException e) {
            return i;
        }
        if (null != coll.insert(doc, WriteConcern.ACKNOWLEDGED)) {
            i++;
        }
        try {
            doc = (DBObject) JSON.parse(docs);
        } catch (JSONParseException e) {
            return i;
        }
        if (null != coll.insert(doc, WriteConcern.ACKNOWLEDGED)) {
            i++;
        }
        return i;
    }

    String query(String dbName, String colName, String qString) {
        db = client.getDB(dbName);
        coll = db.getCollection(colName);
        DBObject query = (DBObject) JSON.parse(qString);
        cur = coll.find(query);
        String data = "[";
        try {
            while (cur.hasNext()) {
                data = data + cur.next().toString() + ",";
            }
        } finally {
            data = data.substring(0, data.length() - 1) + "]"; 
            cur.close();
        }
        return prettyJSON(data);
    }

    private String prettyJSON(String ugly) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(ugly);
        return gson.toJson(je);
    }

    String getIP() {
        return this.server.getSocketAddress().toString();
    }

    String getPort() {
        return Integer.toString(this.server.getPort());
    }

}
