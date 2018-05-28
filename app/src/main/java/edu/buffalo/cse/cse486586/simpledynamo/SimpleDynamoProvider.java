package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static String insert = "1";
    static String query = "2";
    static String delete = "3";
    static String replica_request = "4";
    static String replica_insert_one = "5";
    static String replica_insert_two = "6";
    static String tail_query = "7";
    static String repairing_successor = "8";
    static String repairing_predecessor = "9";
    static String insert_complete = "0";
    static String portStr;
    static Node thisNode;
    HashMap<String, VectorStamp> timeStamp = new HashMap<String, VectorStamp>();
    HashMap<String, Integer> my_msgs = new HashMap<String, Integer>();
    HashMap<String, Integer> pred_msgs = new HashMap<String, Integer>();
    HashMap<String, Integer> pred_pred_msgs = new HashMap<String, Integer>();
    static final int SERVER_PORT = 10000;
    static final String PORT0 = "5554";
    static final String PORT1 = "5556";
    static final String PORT2 = "5558";
    static final String PORT3 = "5560";
    static final String PORT4 = "5562";
    static Node[] ports = new Node[5];
    String tail_query_result = "";

    String failed_port = "";
    String queryResult = "";
    boolean insert_done = false;
    boolean recovery_done = false;
    HashMap<String, String> to_suc_vals = new HashMap<String, String>();
    HashMap<String, String> to_pred_vals = new HashMap<String, String>();
    List<String> to_suc = new ArrayList<String>();
    List<String> to_pred = new ArrayList<String>();
    List<String> to_delete = new ArrayList<String>();
    boolean query_success = false;
    boolean query_success_server = false;
    String replica_query_res = "";
    String replica_query_res_server = "";
    static Semaphore query_generated;
    static Semaphore insert_wait;
    static Semaphore insert_start;
    static Semaphore wait_for_recovery;
    static Semaphore query_replica;
    //static Semaphore query_replica_server;
    static Semaphore replica_insert_done;
    static Semaphore tail_query_sema;
    static Semaphore tail_query_access_sema;
    static final int nodeCount = 5;
    private SQLiteDatabase db;
    private MainDatabaseHelper mOpenHelper;
    private static final String SQL_CREATE_MAIN = "CREATE TABLE IF NOT EXISTS " +
            "dynamtable " + "(" + " key TEXT PRIMARY KEY, " + " value TEXT )";
    private static final String DBNAME = "dynamo";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        while(recovery_done == false) {

        }
        if(selection.equals("@") || selection.equals("*")) {
            SQLiteDatabase db = mOpenHelper.getWritableDatabase();
            mOpenHelper.onCreate(db);
            db.delete("dynamtable",null,null);
            if(selection.equals("*")) {
                String msg = delete + "*";
                String msgLength = "1"; //just one message i.e "*"
                for (Node port : ports) {
                    if(!port.portNo.equals(thisNode.portNo)) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                                port.portNo, delete, msgLength);
                    }
                }
            }
        }
        else {
            String msg = delete + selection;
            String msgLength = Integer.toString(selection.length());
            db.delete("dynamtable","key=?",new String[]{selection});
            for (Node port : ports) {
                if(!port.portNo.equals(thisNode.portNo)) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                            port.portNo, delete, msgLength);
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        //decide the coordinator node based on key's hash value.
        try {
            while(recovery_done == false) {

            }
            Node node_to_Send = null;
            String ky = values.getAsString("key");
            String keyId = genHash(ky);
            VectorStamp vs = new VectorStamp(0, 0, 0);
            insert_start.acquire();
            insert_done = false;
            if (keyId.compareTo(ports[0].id) <= 0) { //this emulator instance itself is the coordinator
                Log.v(TAG,"******Insert handled by port 0 for :"+ky);
                if (thisNode.portNo.equals(ports[0].portNo)) {
                    Log.v(TAG, "Inserting to port 0 key :" +ky);
                    if(my_msgs.containsKey(ky)) {
                       my_msgs.put(ky,my_msgs.get(ky)+1);
                    }
                    else {
                        my_msgs.put(ky,1);
                    }
                    node_to_Send = thisNode;
                    db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    insert_replica1(values.getAsString("key"), values.getAsString("value"),thisNode.portNo);
                } else {
                    String msg = insert + values.getAsString("key") + ":"+values.getAsString("value")+"#"+thisNode.portNo;
                    String msgLength = Integer.toString(values.getAsString("value").length());
                    node_to_Send = ports[0];
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                            ports[0].portNo, insert, msgLength);
                }
            } else if (keyId.compareTo(ports[nodeCount - 1].id) > 0) {
                Log.v(TAG,"******Insert handled by port 0 for :"+ky);
                if (thisNode.portNo.equals(ports[0].portNo)) {
                    Log.v(TAG, "Inserting to port 0 key :" +ky);
                    if(my_msgs.containsKey(ky)) {
                        my_msgs.put(ky,my_msgs.get(ky)+1);
                    }
                    else {
                        my_msgs.put(ky,1);
                    }
                    node_to_Send = thisNode;
                    db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    insert_replica1(values.getAsString("key"), values.getAsString("value"),thisNode.portNo);
                } else {
                    String msg = insert + values.getAsString("key") +":"+ values.getAsString("value")+"#"+thisNode.portNo;
                    String msgLength = Integer.toString(values.getAsString("value").length());
                    node_to_Send = ports[0];
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                            ports[0].portNo, insert, msgLength);
                }
            }
            else {
                for (Node prt : ports) {
                    if(!prt.portNo.equals(ports[0].portNo)) {
                        if (keyId.compareTo(prt.id) <= 0 && keyId.compareTo(prt.predecessor.id) > 0) {
                            Log.v(TAG,"******Insert handled by port "+ prt.portNo+ " for :"+ky);
                            if (thisNode.portNo.equals(prt.portNo)) {
                                Log.v(TAG,"Inserting in the same node : "+ky);
                                if(my_msgs.containsKey(ky)) {
                                    my_msgs.put(ky,my_msgs.get(ky)+1);
                                }
                                else {
                                    my_msgs.put(ky,1);
                                }
                                node_to_Send = thisNode;
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                                insert_replica1(values.getAsString("key"), values.getAsString("value"),thisNode.portNo);
                                break;
                            }
                            else {
                                String msg = insert + values.getAsString("key") +":"+ values.getAsString("value")+"#"+thisNode.portNo;
                                String msgLength = Integer.toString(values.getAsString("value").length());
                                node_to_Send = prt;
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                                        prt.portNo, insert, msgLength);
                                break;
                            }
                        }
                    }
                }
            }
            //insert_wait.acquire();
            //wait for 1 sec for insert acknowledgement
            long send_time = System.currentTimeMillis();
            while((System.currentTimeMillis() - send_time) < 2000) {
                if(insert_done == true) {
                    break;
                }
            }
            if(insert_done == false) {
                if(node_to_Send.portNo.equals(thisNode.portNo)) {
                    insert_replica1(values.getAsString("key"), values.getAsString("value"),thisNode.portNo);
                }
                else {
                    String msg = insert + values.getAsString("key") +":"+ values.getAsString("value")+"#"+thisNode.portNo;
                    String msgLength = Integer.toString(values.getAsString("value").length());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                            node_to_Send.portNo, insert, msgLength);
                }
            }
            insert_start.release();
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Insert failed");
        } catch (InterruptedException e) {
            Log.e(TAG, "Insert failed");
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //getting the port number for the emulator instance.
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //initializing the hash values for ports:-
        try {
            recovery_done = false;
            mOpenHelper = new MainDatabaseHelper(getContext(), DBNAME, null, 1);
            db = mOpenHelper.getWritableDatabase();
            mOpenHelper.onCreate(db);

            query_replica = new Semaphore(0);
            query_generated = new Semaphore(0);
            insert_wait = new Semaphore(0);
            wait_for_recovery = new Semaphore(0);
            insert_start = new Semaphore(1);
            tail_query_access_sema = new Semaphore(1);
            //query_replica_server = new Semaphore(0);
            //query_generated_server = new Semaphore(0);
            replica_insert_done = new Semaphore(0);
            tail_query_sema = new Semaphore(0);
            ports[0] = new Node(PORT0, genHash(PORT0));
            ports[1] = new Node(PORT1, genHash(PORT1));
            ports[2] = new Node(PORT2, genHash(PORT2));
            ports[3] = new Node(PORT3, genHash(PORT3));
            ports[4] = new Node(PORT4, genHash(PORT4));
            Arrays.sort(ports);
            //initializing the successors for the nodes and finding emulator's node instance
            ports[0].predecessor = ports[nodeCount-1];
            ports[0].successor = ports[1];
            if (portStr.compareTo(ports[0].portNo) == 0) {
                thisNode = ports[0];
            }
            for (int i = 1; i < nodeCount; i++) {
                ports[i].predecessor = ports[(i - 1)%5];
                ports[i].successor = ports[(i + 1)%5];
                if (portStr.compareTo(ports[i].portNo) == 0) {
                    thisNode = ports[i];
                }
            }

            //get the messages missed
            new ReplicaRecovery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            Log.v(TAG," %%%%%My nodr prt no : "+thisNode.portNo+" actual portstr :"+portStr);
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "ServerTask failed IOException");
            }



        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Port ID initilization failed");
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        //reference:-http://developer.android.com/
        while(recovery_done == false) {

        }
        selectionArgs = new String[]{selection};
        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables("dynamtable");
        Cursor c;
        if (selection.equals("*") || selection.equals("@")) {
            c = qBuilder.query(db,
                    projection,
                    null,
                    null,
                    null,
                    null,
                    sortOrder);
            MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
            while (c.moveToNext()) {
                mat.addRow(new Object[]{c.getString(0), c.getString(1)});
            }
            if (selection.equals("*")) {
                queryResult = "";
                String msg = query + "*";
                String msgLength = "1"; //just one message i.e "*"
                for (Node port : ports) {
                    if (!port.portNo.equals(thisNode.portNo)) {
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,
                                    port.portNo, query, msgLength);
                            Log.v(TAG, "####Query_gen acquired for star query");
                            query_generated.acquire();
                        } catch (InterruptedException ex) {
                            Log.e(TAG, "Query * failed");
                        }
                    }
                }
                Log.v(TAG, "####Query_gen done for all");
                String rcvd = queryResult;
                queryResult = "";
                if (!rcvd.equals("")) {
                    String[] spltVal = rcvd.split("-");
                    for (String vals : spltVal) {
                        String[] keyVal = vals.split(":");
                        mat.addRow(new Object[]{keyVal[0], keyVal[1]});
                    }
                }
            }
            mat.setNotificationUri(getContext().getContentResolver(), uri);
            return mat;
        } else {
            try {
                String hkey = genHash(selection);
                int wcount = 0;
                for (int i = 0; i < nodeCount; i++) {
                    if (hkey.compareTo(ports[i].id) <= 0 ||
                            ((i == 0) && hkey.compareTo(ports[nodeCount - 1].id) > 0)) {
                        MatrixCursor retMat = new MatrixCursor(new String[]{"key", "value"});
                        //VectorStamp vstmp;
                        if (ports[i].successor.successor.portNo.equals(thisNode.portNo)) {
                            c = qBuilder.query(db,
                                    projection,
                                    "key=?",
                                    selectionArgs,
                                    null,
                                    null,
                                    sortOrder);
                            return c;
                        } else {
                            //replica_query_res = "";
                            tail_query_access_sema.acquire();
                            tail_query_result = "";
                            String msg_to_send = tail_query + selection;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_send,
                                    ports[i].successor.successor.portNo, tail_query, Integer.toString(selection.length()));
                            Log.v(TAG, "####Query_gen acquired for querying " + msg_to_send + " to " + ports[i].portNo);
                            tail_query_sema.acquire();
                            Log.v(TAG, "####Query_gen released for query result : " + replica_query_res);
                            String[] kvalpr = tail_query_result.split(":");
                            retMat.addRow(new Object[]{kvalpr[0], kvalpr[1]});
                            tail_query_access_sema.release();
                            return retMat;
                        }
                    }
                }
            } catch (NoSuchAlgorithmException n) {
                Log.e(TAG, " Query failed");
            } catch (InterruptedException e) {
                Log.e(TAG, " Query failed");
            }
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {
        //reference:-http://developer.android.com/
        /*
         * Instantiates an open helper for the provider's SQLite data repository
         * Do not do database creation and upgrade here.
         */
        MainDatabaseHelper(Context context, String dbname, Cursor cursor, int versionNumber) {
            super(context, DBNAME, null, 1);
        }

        /*
         * Creates the data repository. This is called when the provider attempts to open the
         * repository and SQLite reports that it doesn't exist.
         */
        public void onCreate(SQLiteDatabase db) {
            // Creates the main table
            db.execSQL(SQL_CREATE_MAIN);
        }

        /*
         * Creates the data repository. This is called when the provider attempts to open the
         * repository and SQLite reports that it doesn't exist.
         */
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
    }

    private class Node implements Comparable<Node> {
        String portNo;
        String id;
        Node successor;
        Node predecessor;

        @Override
        public int compareTo(Node node2) {
            return this.id.compareTo(node2.id);
        }

        public Node(String portVal, String hashVal) {
            portNo = portVal;
            id = hashVal;
        }
    }

    private Node give_port_node(String prt) {
        for(Node portNode : ports) {
            if(portNode.portNo.equals(prt)) {
                return portNode;
            }
        }
        return ports[0];
    }

    private class VectorStamp {
        int x, y, z;

        public VectorStamp(int a, int b, int c) {
            x = a;
            y = b;
            z = c;
        }
    }

    private void insert_replica1(String ky, String val,String init_port) {
        Log.v(TAG,"()()()Inserting to replica 1 the key "+ky);
        int order = my_msgs.get(ky);
        String msg1 = replica_insert_one + ky + ":"+ val + ","+order+"#"+init_port ;
        String msgLength = Integer.toString(val.length());
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1,
                    thisNode.successor.portNo, replica_insert_one, msgLength);

    }

    private void insert_replica1_from_server(String ky, String val,String init_port) {
        Log.v(TAG,"()()()Inserting to replica 1 the key "+ky);
        int order = my_msgs.get(ky);
        String msg1 = replica_insert_one + ky + ":"+ val + ","+order+"#"+init_port ;
        String msgLength = Integer.toString(val.length());
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg1,
                thisNode.successor.portNo, replica_insert_one, msgLength);

    }
    private void insert_replica2(String ky, String val,String init_port) {
        Log.v(TAG,"()()()Inserting to replica 2 the key "+ky);
        int order = pred_msgs.get(ky);
        String msg2 = replica_insert_two + ky + ":"+ val + ","+order+"#"+init_port ;
        String msgLength = Integer.toString(val.length());
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2,
                thisNode.successor.portNo, replica_insert_two, msgLength);

    }

    private void insert_replica2_from_server(String ky, String val,String init_port) {
        Log.v(TAG,"()()()Inserting to replica 2 the key "+ky);
        int order = pred_msgs.get(ky);
        String msg2 = replica_insert_two + ky + ":"+ val + ","+order+"#"+init_port ;
        String msgLength = Integer.toString(val.length());
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg2,
                thisNode.successor.portNo, replica_insert_two, msgLength);

    }

    private void get_replicas_successor() {
        String ms_to_send = replica_request + thisNode.portNo;
        Log.v(TAG,"^^^$$$$****Getting the replicas that I missed");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ms_to_send,
                thisNode.successor.portNo, replica_request, "4");
    }
    private void get_replicas_predecessor() {
        String ms_to_send = replica_request + thisNode.portNo;
        Log.v(TAG,"^^^$$$$****Getting the replicas that I missed");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ms_to_send,
                thisNode.predecessor.portNo, replica_request, "4");
    }

    private class ReplicaRecovery extends AsyncTask<String, String, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String ms_to_send = replica_request + thisNode.portNo;
            Log.v(TAG,"^^^$$$$****Getting the replicas that I missed");
            ms_to_send = ms_to_send + "@"+"4";
            try {
                int port = Integer.parseInt(thisNode.predecessor.portNo) * 2;
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                socket1.setSoTimeout(1000);
                DataOutputStream out = new DataOutputStream(socket1.getOutputStream());
                DataInputStream in = new DataInputStream(socket1.getInputStream());
                out.writeUTF(ms_to_send);
                String msg = in.readUTF();

                port = Integer.parseInt(thisNode.successor.portNo) * 2;
                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                socket1.setSoTimeout(1000);
                out = new DataOutputStream(socket1.getOutputStream());
                in = new DataInputStream(socket1.getInputStream());
                out.writeUTF(ms_to_send);
                msg = msg + in.readUTF();

                if(!msg.equals("")) {
                    String[] data = msg.split("&");
                    SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                    Log.v(TAG, "##########The data " + msg);
                    mOpenHelper.onCreate(db);
                    for (String dat : data) {
                        if(!dat.equals("")) {
                            dat = dat.split("#")[0];
                            Log.v(TAG, " The dat is " +dat);
                            String ky = dat.split(":")[0].substring(1);
                            String val = dat.split(":")[1].split(",")[0];
                            ContentValues values = new ContentValues();
                            values.put("key", ky);
                            values.put("value", val);
                            db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                        }
                    }
                }
                recovery_done = true;
            }catch (UnknownHostException e) {
                recovery_done = true;
                Log.e(TAG, "Replica request failed");
            }
            catch (SocketException e) {
                recovery_done = true;
                Log.e(TAG, "Replica request failed");
            }
            catch (IOException e) {
                recovery_done = true;
                Log.e(TAG, "Replica request failed");
            }
            catch (NetworkOnMainThreadException e) {
                recovery_done = true;
                Log.e(TAG, "Replica request failed");
            }
            return null;
        }
    }
    private void get_replicas_predec() {
        String ms_to_send = replica_request + thisNode.portNo;
        Log.v(TAG,"^^^$$$$****Getting the replicas that I missed");
        ms_to_send = ms_to_send + "@"+"4";
        try {
            int port = Integer.parseInt(thisNode.predecessor.portNo) * 2;
            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
            socket1.setSoTimeout(1000);
            DataOutputStream out = new DataOutputStream(socket1.getOutputStream());
            DataInputStream in = new DataInputStream(socket1.getInputStream());
            out.writeUTF(ms_to_send);
            String msg = in.readUTF();

            if(!msg.equals("")) {
                String[] data = msg.split("&");
                SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                Log.v(TAG, "##########The data " + msg);
                mOpenHelper.onCreate(db);
                for (String dat : data) {
                    if(!dat.equals("")) {
                        dat = dat.split("#")[0];
                        Log.v(TAG, " The dat is " +dat);
                        String ky = dat.split(":")[0].substring(1);
                        String val = dat.split(":")[1].split(",")[0];
                        ContentValues values = new ContentValues();
                        values.put("key", ky);
                        values.put("value", val);
                        db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                }
            }
        }catch (UnknownHostException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (SocketException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (IOException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (NetworkOnMainThreadException e) {
            Log.e(TAG, "Replica request failed");
        }
    }

    private void get_replicas_suc() {
        String ms_to_send = replica_request + thisNode.portNo;
        Log.v(TAG,"^^^$$$$****Getting the replicas that I missed");
        ms_to_send = ms_to_send + "@"+"4";
        try {
            int port = Integer.parseInt(thisNode.successor.portNo) * 2;
            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
            socket1.setSoTimeout(1000);
            DataOutputStream out = new DataOutputStream(socket1.getOutputStream());
            DataInputStream in = new DataInputStream(socket1.getInputStream());
            out.writeUTF(ms_to_send);
            String msg = in.readUTF();

            if(!msg.equals("")) {
                String[] data = msg.split("&");
                SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                Log.v(TAG, "##########The data " + msg);
                mOpenHelper.onCreate(db);
                for (String dat : data) {
                    if(!dat.equals("")) {
                        dat = dat.split("#")[0];
                        Log.v(TAG, " The dat is " +dat);
                        String ky = dat.split(":")[0].substring(1);
                        String val = dat.split(":")[1].split(",")[0];
                        ContentValues values = new ContentValues();
                        values.put("key", ky);
                        values.put("value", val);
                        db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                }
            }
        }catch (UnknownHostException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (SocketException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (IOException e) {
            Log.e(TAG, "Replica request failed");
        }
        catch (NetworkOnMainThreadException e) {
            Log.e(TAG, "Replica request failed");
        }
    }

    private class ClientTask extends AsyncTask<String, String, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String msg = "";
            Socket socket1 = null;
            DataOutputStream out = null;
            DataInputStream in = null;

                int port = Integer.parseInt(msgs[1]) * 2;

                String msgToSend = msgs[0];
                String msg_length = msgs[3];
                msgToSend = msgToSend + "@" +msg_length;
                //Reference-docs.oracle.com

                Log.v(TAG, "++++++Client writing for "+msgToSend + " to "+msgs[1]);
            try {
                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                socket1.setSoTimeout(1000);
                out = new DataOutputStream(socket1.getOutputStream());
                in = new DataInputStream(socket1.getInputStream());
                out.writeUTF(msgToSend);
                msg = in.readUTF();
                Log.v(TAG, "++++++Client done for "+msgToSend + " to "+msgs[1]);
                if(msgs[2].equals(replica_request)) {
                    Log.v(TAG,"Releasing wait for recov");
                    wait_for_recovery.release();
                }
                publishProgress(msg, msgs[2]);
                //out.flush();
                //out.close();
                //in.close();
                //socket1.close();
            } catch(SocketTimeoutException ex) {
                Log.e(TAG, "Socket timed out");
                switch (Integer.parseInt(msgs[2])) {
                    case 1:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            //to_suc.add(failed_msg[0]);
                            //sendin middlee of chain
                            String m = replica_insert_one + msgToSend.substring(1);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_one);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }

                    case 2:
                        if(msgToSend.substring(1,2).equals("*")) {
                            query_generated.release();
                        }
                        break;
                    case 4:  //recovery
                        wait_for_recovery.release();
                        break;
                    case 5:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            //sending to tail directly
                            String m = replica_insert_two + msgToSend.substring(1);
                            Log.v(TAG,"Requesting the port of "+msgs[1]+" for "+msgToSend);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_two);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 6:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            String init_port = failed_msg[0].split("#")[1];
                            port = Integer.parseInt(init_port) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            msgToSend = "0in_tail@3";
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            break;
                        } catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 7:
                        try {
                            String port_to_send = give_port_node(msgs[1]).predecessor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for " + msgToSend + " to " + port);

                            publishProgress(msg, tail_query);
                            break;
                        }catch(IOException e3) {
                            Log.e(TAG,"IOexcption in querying");
                            break;
                        }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                switch (Integer.parseInt(msgs[2])) {
                    case 1:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            //to_suc.add(failed_msg[0]);
                            //sendin middlee of chain
                            String m = replica_insert_one + msgToSend.substring(1);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_one);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }

                    case 2:
                        if(msgToSend.substring(1,2).equals("*")) {
                            query_generated.release();
                        }
                        break;
                    case 4:  //recovery
                        wait_for_recovery.release();
                        break;
                    case 5:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            //sending to tail directly
                            String m = replica_insert_two + msgToSend.substring(1);
                            Log.v(TAG,"Requesting the port of "+msgs[1]+" for "+msgToSend);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_two);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 6:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            String init_port = failed_msg[0].split("#")[1];
                            port = Integer.parseInt(init_port) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            msgToSend = "0in_tail@3";
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            break;
                        } catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 7:
                        try {
                            String port_to_send = give_port_node(msgs[1]).predecessor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for " + msgToSend + " to " + port);
                            publishProgress(msg, tail_query);
                            break;
                        }catch(IOException e3) {
                            Log.e(TAG,"IOexcption in querying");
                            break;
                        }
                }
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                switch (Integer.parseInt(msgs[2])) {
                    case 1:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            //to_suc.add(failed_msg[0]);
                            //sendin middlee of chain
                            String m = replica_insert_one + msgToSend.substring(1);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_one);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }

                    case 2:
                        if(msgToSend.substring(1,2).equals("*")) {
                            query_generated.release();
                    }
                    break;
                    case 4:  //recovery
                        Log.v(TAG,"Replica reuqest failed");
                        wait_for_recovery.release();
                        break;
                    case 5:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            //sending to tail directly
                            String m = replica_insert_two + msgToSend.substring(1);
                            Log.v(TAG,"Requesting the port of "+msgs[1]+" for "+msgToSend);
                            String port_to_send = give_port_node(msgs[1]).successor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(m);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for "+m + " to "+port_to_send);
                            publishProgress(msg, replica_insert_two);
                            break;
                        }catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 6:
                        try {
                            out.flush();
                            out.close();
                            in.close();
                            socket1.close();
                            //insert failed message to list
                            String[] failed_msg = msgToSend.split("@");
                            to_suc.add(failed_msg[0]);
                            to_suc_vals.put(failed_msg[0].split(":")[0].substring(1),failed_msg[0]);
                            String init_port = failed_msg[0].split("#")[1];
                            port = Integer.parseInt(init_port) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            msgToSend = "0in_tail@3";
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            break;
                        } catch(IOException e1) {
                            Log.e(TAG,"IOException in client exception");
                            break;
                        }
                    case 7:
                        try {
                            String port_to_send = give_port_node(msgs[1]).predecessor.portNo;
                            port = Integer.parseInt(port_to_send) * 2;
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                            socket1.setSoTimeout(1000);
                            out = new DataOutputStream(socket1.getOutputStream());
                            in = new DataInputStream(socket1.getInputStream());
                            out.writeUTF(msgToSend);
                            msg = in.readUTF();
                            Log.v(TAG, "++++++Client done for " + msgToSend + " to " + port);
                            publishProgress(msg, tail_query);
                            break;
                        }catch(IOException e3) {
                            Log.e(TAG,"IOexcption in querying");
                            break;
                        }
                }
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            switch (Integer.parseInt(strings[1])) {
                case 1:
                    break; //insert
                case 2:
                    queryResult += strings[0];
                    query_generated.release();
                    break;  //query
                case 3: //delete
                    break;
                case 4: //replica request
                    if(!strings[0].equals("")) {
                        String[] data = strings[0].split("&");
                        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                        Log.v(TAG, "##########The data " + strings[0]);
                        mOpenHelper.onCreate(db);
                        for (String dat : data) {
                            if(!dat.equals("")) {
                                dat = dat.split("#")[0];
                                Log.v(TAG, " The dat is " +dat);
                                String ky = dat.split(":")[0].substring(1);
                                String val = dat.split(":")[1].split(",")[0];
                                ContentValues values = new ContentValues();
                                values.put("key", ky);
                                values.put("value", val);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                        }
                        Log.v(TAG,"releasing the wait for recovery");
                        wait_for_recovery.release();
                    }


                    break;
                case 5:
                    /*if(!to_suc.isEmpty()) {
                        String msg_to_send = "";
                        for(String s : to_suc) {
                            msg_to_send = msg_to_send + "&" + s;
                        }
                        msg_to_send = repairing_successor+msg_to_send;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_send,
                                thisNode.successor.portNo, repairing_successor, "4");
                    }*/
                    break;
                case 6:
                    /*if(!to_suc.isEmpty()) {
                        String msg_to_send = "";
                        for(String s : to_suc) {
                            msg_to_send = msg_to_send + "&" + s;
                        }
                        msg_to_send = repairing_successor+msg_to_send;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_send,
                                thisNode.successor.portNo, repairing_successor, "4");
                    }*/
                    break;
                case 7:  //tail query result
                    tail_query_result = strings[0];
                    tail_query_sema.release();
                    break;
                case 8: //successor repair successful
                    //to_suc.clear();
                    break;
                case 9: //predeccessor repair successful
                    //to_pred.clear();
                    break;

            }
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {

                //Reference-docs.oracle.com
                Log.v(TAG,"~~~~~~~~Server task started");
                while (true) {
                    Socket socket = serverSocket.accept();
                    String rcvd = "";
                    SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                    mOpenHelper.onCreate(db);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    String rcd_msg = in.readUTF();
                    while(recovery_done == false) {

                    }
                    Log.v(TAG, "!!!!!!!!!!Rcvd msg at server : "+rcd_msg);
                    int msgLen = Integer.parseInt(rcd_msg.split("@")[1]);
                    String inpMsg = rcd_msg.split("@")[0];
                    String actual_msg = inpMsg.substring(1);
                    int type = Integer.parseInt(inpMsg.substring(0,1));
                    if(type == 0) { //insert complete
                        //insert_wait.release();
                        insert_done = true;
                        out.writeUTF("");
                    }
                    else if(type == 1) {  //handling insert
                        String[] vals_msg = actual_msg.split("#");
                        String ky = vals_msg[0].split(":")[0];
                        String val = vals_msg[0].split(":")[1];

                        ContentValues values = new ContentValues();
                        values.put("key",ky);
                        values.put("value",val);
                        Log.v(TAG,"Inserting key : "+ky);
                        if(my_msgs.containsKey(ky)) {
                            my_msgs.put(ky,my_msgs.get(ky)+1);
                        }
                        else {
                            my_msgs.put(ky,1);
                        }
                        db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                        insert_replica1_from_server(ky, val,vals_msg[1]);
                        out.writeUTF("");
                    }
                    else if(type == 2) { //handling query request
                        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                        qBuilder.setTables("dynamtable");
                        String selection = actual_msg;
                        String[]  selectionArgs = new String[]{selection};
                        if(msgLen == 1 && actual_msg.equals("*")) {

                            Cursor resultCursor = qBuilder.query(db,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null);
                            rcvd = "";
                            while (resultCursor.moveToNext()) {
                                rcvd = rcvd + resultCursor.getString(0) + ":" + resultCursor.getString(1);
                                rcvd = rcvd + "-";

                            }
                            out.writeUTF(rcvd);

                        }
                        else {
                            //reading from R-1 nodes i.e 1 more in our case.

                                String retcord = "";
                                Cursor c = qBuilder.query(db,
                                        null,
                                        "key=?",
                                        selectionArgs,
                                        null,
                                        null,
                                        null);
                                while (c.moveToNext()) {
                                    retcord = retcord + c.getString(0) + ":" + c.getString(1);
                                }
                                    out.writeUTF(retcord);
                            }
                        }

                    else if(type == 3) {    //handling delete

                        SQLiteDatabase dbdel = mOpenHelper.getWritableDatabase();
                        mOpenHelper.onCreate(dbdel);
                        String selection = actual_msg;
                        if(msgLen == 1 && actual_msg.equals("*")) {
                            dbdel.delete("dynamtable",null,null);
                        }
                        else {
                            dbdel.delete("dynamtable","key=?",new String[]{selection});
                        }
                        out.writeUTF("");
                    }

                    else if(type == 4) {    //to handle replica request
                        String requesting_prt = actual_msg;
                        String msg_to_send = "";
                        if(requesting_prt.equals(thisNode.successor.portNo)) {

                            if(!to_suc_vals.isEmpty()) {

                                for (String key : to_suc_vals.keySet()) {
                                    msg_to_send = msg_to_send + "&" + to_suc_vals.get(key);
                                }
                            }
                            to_suc_vals.clear();
                        }
                        else if(requesting_prt.equals(thisNode.predecessor.portNo)) {
                            if(!to_pred_vals.isEmpty()) {

                                for (String key : to_pred_vals.keySet()) {
                                    msg_to_send = msg_to_send + "&" + "1"+to_pred_vals.get(key);
                                }
                            }
                            to_pred_vals.clear();
                        }
                        Log.v(TAG, "@@@@@The replicated message sent "+msg_to_send);
                        out.writeUTF(msg_to_send);
                    }
                    else if(type == 5) {  //handling replication
                        String[] val_pairs = actual_msg.split("#");
                        String[] repl_msg = val_pairs[0].split(",");
                        String ky = repl_msg[0].split(":")[0];
                        String val = repl_msg[0].split(":")[1];

                        ContentValues values = new ContentValues();
                        if(to_pred_vals.containsKey(actual_msg.split(":")[0])) {
                            to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                        }
                        if(to_suc_vals.containsKey(actual_msg.split(":")[0])) {
                            to_suc_vals.put(actual_msg.split(":")[0],"6"+actual_msg);
                        }
                        values.put("key", ky);
                        values.put("value", val);
                        if(repl_msg.length == 2) {
                            Integer order = Integer.parseInt(repl_msg[1]);
                            if(pred_msgs.containsKey(ky)) {
                                if(order > pred_msgs.get(ky)) {
                                    pred_msgs.put(ky,order);

                                }
                                //key missing in head so send him keys
                                else {
                                    Log.v(TAG,"Adding the missing msg "+actual_msg);
                                    to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                                    to_pred.add(actual_msg);
                                }
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            else {
                                pred_msgs.put(ky,order);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                        }
                        else {
                            if(pred_msgs.containsKey(ky)) {
                                pred_msgs.put(ky,pred_msgs.get(ky)+1);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            else {
                                pred_msgs.put(ky,1);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            Log.v(TAG,"Adding the missing msg from second option"+actual_msg);
                            to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                            to_pred.add(actual_msg);
                        }
                        insert_replica2_from_server(ky, val,val_pairs[1]);

                        out.writeUTF("");
                    }
                    else if(type == 6) {  //handling replication
                        String[] val_pairs = actual_msg.split("#");
                        String[] repl_msg = val_pairs[0].split(",");
                        String ky = repl_msg[0].split(":")[0];
                        String val = repl_msg[0].split(":")[1];
                        ContentValues values = new ContentValues();
                        if(to_pred_vals.containsKey(actual_msg.split(":")[0])) {
                            to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                        }
                        values.put("key", ky);
                        values.put("value", val);
                        if(repl_msg.length == 2) {
                            Integer order = Integer.parseInt(repl_msg[1]);
                            if(pred_pred_msgs.containsKey(ky)) {
                                if(order > pred_pred_msgs.get(ky)) {
                                    pred_pred_msgs.put(ky,order);

                                }
                                //key missing in head so send him keys
                                else {
                                    Log.v(TAG,"Adding the missing msg "+actual_msg);
                                    to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                                    to_pred.add(actual_msg);
                                }
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            else {
                                pred_pred_msgs.put(ky,order);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                        }
                        else {
                            if(pred_pred_msgs.containsKey(ky)) {
                                pred_pred_msgs.put(ky,pred_pred_msgs.get(ky)+1);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            else {
                                pred_pred_msgs.put(ky,1);
                                db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            Log.v(TAG,"Adding the missing msg from second option"+actual_msg);
                            to_pred_vals.put(actual_msg.split(":")[0],actual_msg);
                            to_pred.add(actual_msg);
                        }
                        out.writeUTF("");
                        String msg_to_send = insert_complete+"in_tail";
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_to_send,
                                val_pairs[1], insert_complete, "3");
                    }
                    else if(type == 7) {  //handling tail query
                        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                        qBuilder.setTables("dynamtable");
                        String selection = actual_msg;
                        String[]  selectionArgs = new String[]{selection};
                        String retcord = "";
                        Cursor c = qBuilder.query(db,
                                null,
                                "key=?",
                                selectionArgs,
                                null,
                                null,
                                null);
                        while (c.moveToNext()) {
                            retcord = retcord + c.getString(0) + ":" + c.getString(1);
                        }
                        Log.v(TAG,"Output from tail "+retcord);
                        out.writeUTF(retcord);
                    }
                    else if(type == 8) { //repairing successor
                        String[] to_insrt = actual_msg.split(",");
                        String[] msgs_missed = to_insrt[0].split("&");
                        for(String msg : msgs_missed) {
                            String ky = msg.split(":")[0];
                            String val = msg.split(":")[1];
                            ContentValues values = new ContentValues();
                            values.put("key", ky);
                            values.put("value", val);
                            db.insertWithOnConflict("dynamtable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                        }
                        out.writeUTF("");
                    }
                    else if(type == 9) { //repairing predecessor

                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "ServerTask failed IOException");
                return null;
            }
        }
    }
}

