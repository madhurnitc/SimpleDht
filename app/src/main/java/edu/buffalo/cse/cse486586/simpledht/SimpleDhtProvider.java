package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.chord_ring;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.message;

public class SimpleDhtProvider extends ContentProvider {
    public String successor = null;
    public String predecessor = null;
    static boolean got_notifications_from_others=true;
    static String myPort;
    static HashMap<String, String> key_value=new HashMap<String, String>();
    // static String emulatorId=SimpleDhtActivity.emulatorId;
    static final String REMOTE_PORT0 = "11108";
    public static final int DATABASE_VERSION = 2;
    public static final String DATABASE_NAME = "group_messenger";
    public static final String TABLE_NAME = "group_messenger_details";
    public static SQLiteDatabase database;
    public static String message = "Join Message";
    //public static int NUMBER_OF_AVDS=5;
    public static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE," + " value TEXT NOT NULL);";
    public static TreeMap<String, String> chord_ring = new TreeMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (selection.equals("\"@\"")) {
            database.execSQL("delete from " + TABLE_NAME);
        }
        if (selection.equals("\"*\"")) {
            Message message = new Message();
            message.setMessageOriginatedFrom(myPort);
            try {
                message.setStatus(Message.Status.DELETE_ALL.toString());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                Log.v(" Message Generated, Status:", message.getStatus().toString());
                Log.v(" ....myPort ", " " + myPort);
                clientTaskOutputStream.writeObject(message);
                Log.v("Encountered Endless while,", "Will wait now till others send their key_value pairs.....");
                while (Status.not_received_from_others) {
                }
                Log.v("While Over......","Deleting from Originator");
                database.execSQL("delete from " + TABLE_NAME);
                Status.not_received_from_others=true;
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

        } else
            database.delete(TABLE_NAME, "key=" + "'" + selection + "'", selectionArgs);
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
        Uri uriToReturn = null;
        try {
            Log.v(" In Insert ", "");
            String key = values.get("key").toString();
            Log.v(" Key_value ", key + "_" + values.get("value").toString());
            String keyId = genHash(key);
            String nodeId = genHash(getEmulatorIdFromPort(myPort));

            Log.v("KeyId is ", keyId);
            Log.v("NodeId is ", nodeId);
            Log.v("Successor Port  " + successor, "Predeccesor Port " + predecessor);
            String succ = "";
            String pred = "";
            if (predecessor != null && successor != null) {
                succ = genHash(getEmulatorIdFromPort(successor));
                Log.v("Node's SUCCESSOR is ", succ);
                pred = genHash(getEmulatorIdFromPort(predecessor));
                Log.v("Node;s PREDECESSOR is ", pred);
            } else {
                Log.v("SUCCESSOR PREDECESSOR  ", "NULL-----");
            }
            long row_id = 0;
            if (predecessor == null && successor == null)
                Log.v("Condition 3 is true-------------------------", "**************");
            else {
                if (((keyId.compareTo(nodeId) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))) {
                    Log.v("Condition 1 is true-------------------------", "**************");
                } else if ((((keyId.compareTo(nodeId) > 0)) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))) {
                    Log.v("Condition 2 is true-------------------------", "**************");
                } else if ((keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0) && ((keyId.compareTo(nodeId) < 0) || (keyId.compareTo(nodeId)) == 0))
                    Log.v("Condition 4 is true-------------------------", "**************");
            }
            if ((predecessor == null && successor == null)) {
                Log.v(" Correct key is found. Inserting at port...... ", myPort);
                row_id = database.insertWithOnConflict(TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("insert", values.toString());
                uriToReturn = ContentUris.withAppendedId(uri, row_id);

            } else if (((keyId.compareTo(nodeId) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))
                    || (((keyId.compareTo(nodeId) > 0)) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0)) ||
                    ((keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0) && ((keyId.compareTo(nodeId) < 0) || (keyId.compareTo(nodeId)) == 0))) {
                Log.v(" Correct key is found. Inserting at port...... ", myPort);
                row_id = database.insertWithOnConflict(TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("insert", values.toString());
                uriToReturn = ContentUris.withAppendedId(uri, row_id);
            } else {
                Log.v(" Forwarding to successor...... ", successor);
                Message message = new Message();
                message.setStatus(Message.Status.WRITE_MESSAGE.toString());
                String msgToSend = key + "_" + values.get("value").toString();
                message.setMessage(msgToSend);
                message.setMessageFrom(myPort);
                message.setGreatestPortSeen(myPort);
                message.setMessageOriginatedFrom(myPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientTaskOutputStream.writeObject(message);
                Log.v("End Insert: Forwarded Message Generated, Status:", message.getStatus().toString());
                clientTaskOutputStream.close();
                socket.close();

            }

//            row_id = database.insertWithOnConflict(TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);


        } catch (Exception e) {
            e.printStackTrace();
        }
        return uriToReturn;

    }

    @Override
    public boolean onCreate() {
        final int SERVER_PORT = 10000;
        try {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            //emulatorId=portStr;
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            Log.v("Sender port on start in server socket", " " + myPort);
            if (myPort.equals(REMOTE_PORT0))
                chord_ring.put(genHash(getEmulatorIdFromPort(REMOTE_PORT0)), REMOTE_PORT0);
            Log.v("Chord ring size ", " " + chord_ring.size());

            Log.v("", " Create a ServerSocket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
        } catch (IOException e) {
            Log.e("", "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            Log.e("", e.toString());
        }
        Context context = getContext();
        SQLiteDatabaseHelper sqLiteDatabaseHelper = new SQLiteDatabaseHelper(context);
        database = sqLiteDatabaseHelper.getWritableDatabase();
        if (database == null)
            return false;
        return true;

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        static final int SERVER_PORT = 10000;
        private static final String TAG = "ClientTask";

        @Override
        protected Void doInBackground(String... msgs) {

            try {
                Log.v("Client Task Starts....myPort ", " " + myPort);

                Log.v("ClientTask", "Generating Join Message");
                Message message = new Message();
                message.setStatus(Message.Status.JOIN.toString());
                String msgToSend = msgs[0];
                message.setMessage(msgToSend);
                message.setMessageFrom(myPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));

                ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientTaskOutputStream.writeObject(message);
                Log.v("ClientTask Message Generated, Status:", message.getStatus().toString());
                Log.v("Client Task Ends....myPort ", " " + myPort);
                clientTaskOutputStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> implements Serializable {

        private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        private Uri buildUri(String scheme, String authority) {
            Log.v("", "Build URI method in server");
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v("ServerSocket ", "In Do background");
            ServerSocket serverSocket = sockets[0];
            try {
                Uri uriToReturn = null;
                Log.v("ServerSocket ", "In Try");
                while (true) {
                    Log.v("ServerSocket ", "In While");
                    Socket clientSocket = serverSocket.accept();
                    Log.v("ServerSocket ", "Accepted Server Socket");
                    ObjectInputStream serverTaskInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    Message message_received = (Message) serverTaskInputStream.readObject();
                    Log.v("ServerSocket ", "Read Message");
                    Log.v("I am ", myPort);

                    if (message_received.getStatus().equals(Message.Status.JOIN.toString())) {
                        Log.v("Message Status in Join is   ", message_received.getStatus().toString());

                        createAndAddNode(message_received.getMessageFrom());
                    }
                    if (message_received.getStatus().equals(Message.Status.UPDATE_SUCC_PRE.toString())) {
                        Log.v("Message Status in update is   ", message_received.getStatus().toString());

                        String get_Previous_Next = message_received.getUpdateSuccPred();
                        String[] prev_next = get_Previous_Next.split("_");
                        predecessor = prev_next[0];
                        successor = prev_next[1];
                        Log.v("Predecessor ", predecessor);
                        Log.v("Successor ", successor);

                    }
                    if (message_received.getStatus().equals(Message.Status.DELETE_ALL.toString())) {
                        Log.v("Message Status in update is   ", message_received.getStatus().toString());

                        if(message_received.getMessageOriginatedFrom().equals(myPort)) {

                            Log.v("Deleted from everyone ", "Now going to delete self");
                            edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others = false;
                            Log.v("Status.not_received_from_others After ", edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others + "");

                        }
                        else {
                            database.execSQL("delete from "+ TABLE_NAME);
                            delete(mUri,"\"@\"", null);
                            Log.v("Still Deleting Others ", "..............");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            clientTaskOutputStream.writeObject(message_received);
                            Log.v("ServerTask Message Generated, Status:", message_received.getStatus().toString());
                            Log.v("Server Task Ends....myPort ", " " + myPort);
                            clientTaskOutputStream.close();
                            socket.close();

                        }


                        }
                    if (message_received.getStatus().equals(Message.Status.QUERY_ALL.toString())) {
                        Log.v("Message Status in QUERY is   ", message_received.getStatus().toString());
                        if(message_received.getMessageOriginatedFrom().equals(myPort)) {
                            key_value.clear();
                            key_value=message_received.getKey_value();
                            Log.v("My key_Value &&&&&&&&&&&&&&&& ", key_value.entrySet().toString());
                            edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others = false;
                            Log.v("Status.not_received_from_others After ", edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others + "");

                        }
                            else {
                            Log.v("Originator was   ",message_received.getMessageOriginatedFrom());
                            Log.v("In Port "+myPort+ " Emulator "+getEmulatorIdFromPort(myPort), "Calling Query for this AVD.....");
                            Cursor cursor = database.rawQuery("Select key,value from group_messenger_details where value IS NOT NULL", null);
                            Log.v("Back...the results returned ",cursor.getCount()+"");
                            HashMap<String, String> hashmap=message_received.getKey_value();

                            if (cursor != null) {
                                if (cursor.moveToFirst()) {
                                    do {

                                        hashmap.put(cursor.getString(cursor.getColumnIndex("key")), cursor.getString(cursor.getColumnIndex("value")));
                                        Log.v("key", cursor.getString(cursor.getColumnIndex("key")));
                                        Log.v("value", cursor.getString(cursor.getColumnIndex("value")));
                                        // "Title" is the field name(column) of the Table
                                    } while (cursor.moveToNext());
                                }
                            }
                            message_received.setKey_value(hashmap);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            clientTaskOutputStream.writeObject(message_received);
                            Log.v("ServerTask Message Generated, Status:", message_received.getStatus().toString());
                            Log.v("Server Task Ends....myPort ", " " + myPort);
                            clientTaskOutputStream.close();
                            socket.close();
                        }

                    }
                    if (message_received.getStatus().equals(Message.Status.QUERY_ONE.toString())) {
                        Log.v("Message Status in QUERY_ONE is   ", message_received.getStatus().toString());
                            Log.v("In Port "+myPort+ " Emulator "+getEmulatorIdFromPort(myPort), "Calling Query_ONE for this AVD.....");
                        Log.v("Originator of message" + message_received.getMessageOriginatedFrom(), " MyPort is " + myPort);
                        if(message_received.getMessageOriginatedFrom().equals(myPort)) {
                            key_value.clear();
                            key_value=message_received.getKey_value();
                            Log.v("My key_Value &&&&&&&&&&&&&&&& ", key_value.entrySet().toString());
                            edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others = false;
                            Log.v("Status.not_received_from_others After ", edu.buffalo.cse.cse486586.simpledht.Status.not_received_from_others + "");

                        }

                        else {
                            //Cursor cursor = query(buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider"), null, message_received.getLookingForKey(), null, "key" + " ASC");
                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(TABLE_NAME);

                            Cursor cursor = queryBuilder.query(database, null, "key=" + "'" + message_received.getLookingForKey() + "'", null,
                                    null, null, "key ASC");
                            Log.v("Back...the results returned for QUERY_ONE ", cursor.getCount() + "");
                            if (cursor != null) {
                                Log.v("got_notifications_from_others Before", got_notifications_from_others + "");

                                if (cursor.getCount() > 0) {
                                    HashMap<String, String> hashmap=new HashMap<String, String>();
                                    cursor.moveToFirst();
                                    Log.v("Found the key value pair", "");
                                    do {

                                        hashmap.put(cursor.getString(cursor.getColumnIndex("key")), cursor.getString(cursor.getColumnIndex("value")));
                                        Log.v("key", cursor.getString(cursor.getColumnIndex("key")));
                                        Log.v("value", cursor.getString(cursor.getColumnIndex("value")));
                                        // "Title" is the field name(column) of the Table
                                    } while (cursor.moveToNext());
                                    message_received.setKey_value(hashmap);
                                    Log.v("Setting key value in sending message ", message_received.getKey_value().entrySet().toString());
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message_received.getMessageOriginatedFrom()));
                                    ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                    clientTaskOutputStream.writeObject(message_received);
                                    Log.v("ServerTask Message Generated, Status:", message_received.getStatus().toString());
                                    Log.v("Server Task Ends....myPort ", " " + myPort);
                                    clientTaskOutputStream.close();
                                    socket.close();

                                }
                                else
                                {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                                    ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                    clientTaskOutputStream.writeObject(message_received);
                                    Log.v("Going to successor for query", "");
                                    Log.v("ServerTask Message Generated, Status:", message_received.getStatus().toString());
                                    Log.v("Server Task Ends....myPort ", " " + myPort);
                                    clientTaskOutputStream.close();
                                    socket.close();
                                }
                            }
                        }



                    }

                    if (message_received.getStatus().equals(Message.Status.WRITE_MESSAGE.toString())) {
                        Log.v(" In Write messsage ", " start");
                        String[] key_value = message_received.getMessage().split("_");
                        String key = key_value[0];
                        String keyId = genHash(key);
                        String nodeId = genHash(getEmulatorIdFromPort(myPort));
                        Log.v("Key after split is ", key);

                        Log.v(" Key Value to be stored is ", message_received.getMessage());
                        Log.v(" NodeId Is ", nodeId);
                        Log.v(" KeyId Is ", keyId);
                        Log.v("PREDECESSOR ", genHash(getEmulatorIdFromPort(predecessor)));
                        Log.v("SUCCESSOR ", genHash(getEmulatorIdFromPort(successor)));

                        String value = key_value[1];

                        long row_id = 0;

                        ContentValues values = null;
                        if (message_received.getGreatestPortSeen().compareTo(myPort) > 0)
                            message_received.setGreatestPortSeen(myPort);
                        if (predecessor == null && successor == null)
                            Log.v("Condition 3 is true-------------------------", "**************");
                        else {
                            if (((keyId.compareTo(nodeId) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))) {
                                Log.v("Condition 1 is true-------------------------", "**************");
                            } else if ((((keyId.compareTo(nodeId) > 0)) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))) {
                                Log.v("Condition 2 is true-------------------------", "**************");
                            } else if ((keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && ((keyId.compareTo(nodeId) < 0) || (keyId.compareTo(nodeId)) == 0))
                                Log.v("Condition 4 is true-------------------------", "**************");
                        }
                        if (((keyId.compareTo(nodeId) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0))

                                ||
                                (((keyId.compareTo(nodeId) > 0)) && (nodeId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) < 0) && (keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0)) ||
                                ((keyId.compareTo(genHash(getEmulatorIdFromPort(predecessor))) > 0) && ((keyId.compareTo(nodeId) < 0) || (keyId.compareTo(nodeId)) == 0))) {
                            Log.v(" Correct node for message found ", nodeId);
                            values = new ContentValues();
                            values.put("key", key);
                            values.put("value", value);
                            row_id = database.insertWithOnConflict(TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                            Log.v("insert", values.toString());
                            uriToReturn = ContentUris.withAppendedId(mUri, row_id);
                        } else {
                            Log.v(" Forwarding message to correct Node ", "");
                            message_received.setMessageFrom(myPort);
                            /*if(message_received.getMessageOriginatedFrom().equals(myPort)) {
                                Log.v (" Message status changed to Final:  ",Message.Status.FINAL_WRITE.toString() );
                                message_received.setStatus(Message.Status.FINAL_WRITE.toString());
                            }*/
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            clientTaskOutputStream.writeObject(message_received);
                            Log.v("ServerTask Message Generated, Status:", message_received.getStatus().toString());
                            Log.v("Server Task Ends....myPort ", " " + myPort);
                            clientTaskOutputStream.close();
                            socket.close();

                        }
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void createAndAddNode(String messageFrom) {
        Log.v("createAndAddNode ", "Start");
        String emulatorID = getEmulatorIdFromPort(messageFrom);
        String nodeID = "";
        String prev_next;
        try {

            nodeID = genHash(emulatorID);
            String prev = "";
            String next = "";
            chord_ring.put(nodeID, messageFrom);


            Log.v("createAndAddNode ", "All AVDS have arrived");
            ArrayList<String> port_List = new ArrayList<String>();
            for (Map.Entry<String, String> entry : chord_ring.entrySet()) {
                String key = entry.getKey();
                String port = entry.getValue();
                port_List.add(port);


            }
            Log.v("List of emulators", "List of emulators");
            for (int i = 0; i < port_List.size(); i++) {
                Log.v("", port_List.get(i));
            }
            if (port_List.size() > 1) {
                for (int i = 0; i < port_List.size(); i++) {

                    if (i == 0) {
                        prev = port_List.get(port_List.size() - 1);
                        next = port_List.get(i + 1);

                    }
                    if (i == port_List.size() - 1) {
                        prev = port_List.get(i - 1);
                        next = port_List.get(0);
                    }
                    if (i != 0 && i != port_List.size() - 1) {
                        prev = port_List.get(i - 1);
                        next = port_List.get(i + 1);
                    }
                    prev_next = prev + "_" + next;
                    Message message = new Message();
                    message.setUpdateSuccPred(prev_next);
                    message.setStatus(Message.Status.UPDATE_SUCC_PRE.toString());
                    message.setMessageFrom(port_List.get(i));
                    Log.v("createAndAddNode ", "Creating Sockets...");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port_List.get(i)));
                    ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    clientTaskOutputStream.writeObject(message);
                    clientTaskOutputStream.close();
                    socket.close();

                }
            }

        } catch (NoSuchAlgorithmException e) {
            Log.v("Exception ", e.toString());
            e.printStackTrace();
        } catch (UnknownHostException e) {
            Log.v("Exception ", e.toString());
            e.printStackTrace();
        } catch (IOException e) {
            Log.v("Exception ", e.toString());
            e.printStackTrace();
        }
    }

    private String getEmulatorIdFromPort(String messageFrom) {
        return String.valueOf((Integer.parseInt(messageFrom) / 2));
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        Log.v(" Inside Query: ", "");
        // TODO Auto-generated method stub
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor=null;
        MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });

        if(predecessor==null && successor==null)
        {
            Log.v(" Inside Query: ", "There is only one NODE");

            if (selection.equals("\"*\"") || selection.equals("\"@\""))
                cursor = database.rawQuery("Select key,value from group_messenger_details where value IS NOT NULL", null);
            else
                cursor = queryBuilder.query(database, projection, "key=" + "'" + selection + "'", selectionArgs,
                        null, null, sortOrder);


        }
        else {
            if (selection.equals("\"@\""))
            {
                Log.v("Query: ", "in @");
                cursor = database.rawQuery("Select key,value from group_messenger_details where value IS NOT NULL", null);
                HashMap<String, String> key_value_hashmap = new HashMap<String, String>();
                Log.v("Size of Cursor", cursor.getCount()+"");
                  return cursor;
            }
            else
            if (selection.equals("\"*\""))
            {
                //To Do

                Message message=new Message();
                message.setMessageOriginatedFrom(myPort);
                try {
                    message.setStatus(Message.Status.QUERY_ALL.toString());
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                    ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    Log.v(" Message Generated, Status:", message.getStatus().toString());
                    Log.v(" ....myPort ", " " + myPort);
                    message.setKey_value(key_value);
                    clientTaskOutputStream.writeObject(message);
                    Log.v("Encountered Endless while,","Will wait now till others send their key_value pairs.....");
                    while(Status.not_received_from_others){}
                   // Log.v("Proceed and collect response from others,","Will continue now.....");
                    for (Map.Entry<String, String> entry : key_value.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        Log.v("Key_Value--->", key+" _ "+value);
                        matrixCursor.addRow(new Object[] { key, value });
                        // ...
                    }
                    Log.v("Proceed and collect response from others,","Will continue now.....");

                    cursor = database.rawQuery("Select key,value from group_messenger_details where value IS NOT NULL", null);
                    //HashMap<String, String> key_value_hashmap = new HashMap<String, String>();
                    Log.v("Size of Cursor after collecting from all ", matrixCursor.getCount()+"");
                    Log.v("Iterating through self: Total Entries ", cursor.getCount()+"");
                    if (cursor != null) {
                        if (cursor.moveToFirst()) {
                            do {
                                String key =cursor.getString(cursor.getColumnIndex("key"));
                                String value = cursor.getString(cursor.getColumnIndex("value"));
                                matrixCursor.addRow(new Object[] { key, value });
                                Log.v("key", cursor.getString(cursor.getColumnIndex("key")));
                                Log.v("value", cursor.getString(cursor.getColumnIndex("value")));
                                // "Title" is the field name(column) of the Table
                            } while (cursor.moveToNext());
                        }
                    }
                    clientTaskOutputStream.close();
                    socket.close();
                    //key_value.clear();
                    Status.not_received_from_others=true;
                    Log.v("Matrix Cursor Size", matrixCursor.getCount()+"");
                    return matrixCursor;


                }
                catch(Exception e)
                {
                    Log.e("Exception in QUERY", e.toString());
                }

            }
             else
            {
                Log.v(" Looking for Single key......", selection);

                cursor = queryBuilder.query(database, projection, "key=" + "'" + selection + "'", selectionArgs,
                        null, null, sortOrder);

                if(cursor.getCount()<=0) {
                    Message message = new Message();
                    message.setMessageOriginatedFrom(myPort);
                    try {
                        message.setStatus(Message.Status.QUERY_ONE.toString());
                        message.setLookingForKey(selection);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                        ObjectOutputStream clientTaskOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        clientTaskOutputStream.writeObject(message);
                        Log.v("not_received_from_others",Status.not_received_from_others+"");
                        while(Status.not_received_from_others){
                            //Log.v("", "Not recieved notification");
                        }
                Log.v("Left the while Proceded", key_value.size()+"");
                    if(key_value.size()>0)
                    {
                        TreeMap<String, String> myMap = new TreeMap<String, String>(key_value);
                        String key = myMap.firstEntry().getKey();
                        String value = key_value.get(key);
                        Log.v("Returning matrix Cursor key value pair  ","Key _ "+key+ "Value _ "+value);
                        matrixCursor.addRow(new Object[] { key, value });
                        //key_value.clear();
                        Status.not_received_from_others=true;
                        return matrixCursor;

                    }
                        Log.v(" Message Generated, Status:", message.getStatus().toString());
                        Log.v("....myPort ", " " + myPort);

                        clientTaskOutputStream.close();
                        socket.close();
                       /* Log.v("Encountered Endless while,", "Will wait now till others send their key_value pairs.....");
                        Log.v("Proceed and collect response from others,", "Will continue now.....");
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
*/

                    }
                    catch(Exception e)
                    {
                    Log.e("Exception in Query One", e.toString());}
                }
                }
                    return cursor;
        }
            cursor.setNotificationUri(getContext().getContentResolver(), uri);


        Log.v("query", selection);
        Log.v("query_count", cursor.getCount() + "");
       // wait
        //{@}
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private static class SQLiteDatabaseHelper extends SQLiteOpenHelper {
        // References for below code from http://developer.android.com/guide/topics/data/data-storage.html#db
        SQLiteDatabaseHelper(Context context) {

            super(context, DATABASE_NAME, null, DATABASE_VERSION);

        }

        @Override
        public void onCreate(SQLiteDatabase database) {
            database.execSQL(CREATE_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            onCreate(db);
        }
    }
}
