package test;

import main.*;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by luke on 9/27/14.
 */
public class DataStoreTest {

    private static boolean servers_active = false;
    private static HTTPServer discovery_server;
    private static HTTPServer data_store_server1;
    private static String discovery_ip;
    private static int discovery_port;
    private static String data_store_ip1;
    private static int data_store_port1;
    private static Socket data_store_socket1a;
    private static OutputStream data_store_out1a;
    private static BufferedReader data_store_in1a;
    private static Socket data_store_socket1b;
    private static OutputStream data_store_out1b;
    private static BufferedReader data_store_in1b;
    private static Socket discovery_socket;
    private static OutputStream discovery_out;
    private static BufferedReader discovery_in;
    private static TestServerThread discovery_server_thread;
    private static TestServerThread data_store_server_thread1;
    private static VectorTimestamp vector_timestamp;

    final static Logger logger = Logger.getLogger(DataStoreTest.class);

    private static void setupServer() {

        if (!servers_active) {

            // Initialize one discovery server thread and one data store server thread
            discovery_server = new HTTPServer(Constants.Config.DISCOVERY, null, true, null);

            discovery_server_thread = new TestServerThread(discovery_server, null, null);
            discovery_server_thread.start();

            data_store_server1 = new HTTPServer(Constants.Config.DATASTORE, "1", true, null);

            data_store_server_thread1 = new TestServerThread(data_store_server1, null, null);
            data_store_server_thread1.start();

            waitBetweenTests();
            waitBetweenTests();

            // Retrieve locations for each server
            String[] server_params = SystemUtility.getServerConfig(Constants.Config.DISCOVERY, null, true);
            discovery_ip = server_params[0];
            discovery_port = Integer.decode(server_params[1]);

            server_params = SystemUtility.getServerConfig(Constants.Config.DATASTORE, "1", true);
            data_store_ip1 = server_params[0];
            data_store_port1 = Integer.decode(server_params[1]);

            servers_active = true;
        }

        try {

            data_store_socket1a = new Socket(data_store_ip1, data_store_port1);
            data_store_out1a = data_store_socket1a.getOutputStream();
            data_store_in1a = new BufferedReader(new InputStreamReader(data_store_socket1a.getInputStream()));

            data_store_socket1b = new Socket(data_store_ip1, data_store_port1);
            data_store_out1b = data_store_socket1a.getOutputStream();
            data_store_in1b = new BufferedReader(new InputStreamReader(data_store_socket1a.getInputStream()));

            discovery_socket = new Socket(discovery_ip, discovery_port);
            discovery_out = discovery_socket.getOutputStream();
            discovery_in = new BufferedReader(new InputStreamReader(discovery_socket.getInputStream()));
        } catch (IOException e) {

            Assert.fail("Unable to open socket");
        }
    }

    @Test
    public void postBasicTweetShouldReturnCreated() throws ParseException {

        setupServer();
        String request_tweet = "{\"tweet\": \"this is a #tweet\", \"hashtags\":[\"tweet\"]}";

        String request = HTTPConstants.HTTPMethod.POST + " " +
                "/" + Constants.Tokens.TWEETS + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + request_tweet.getBytes().length + "\n\n" + request_tweet;

        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);
        JSONObject response_body = response.getBody();
        vector_timestamp = new VectorTimestamp(new HashMap<String, String>((JSONObject) response_body.get(Constants.Tokens.TIMESTAMP)));

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.CREATED));
    }

    @Test
    public void searchDataStoreWithValidVersionShouldReturnNotModified() throws ParseException {

        setupServer();

        String query = "?q=tweet&v=1";
        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        String timestamp_json = timestamp_json_obj.toJSONString();

        String request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;

        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);
        JSONObject response_body_map = response.getBody();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.NOTMODIFIED));
        Assert.assertTrue(response_body_map.get(Constants.Tokens.STATUS).equals(Constants.Messages.NOTMODIFIED));
    }

    @Test
    public void searchDataStoreWithInvalidVersionShouldReturnOK() throws ParseException {

        setupServer();

        String query = "?q=tweet&v=0";
        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        String timestamp_json = timestamp_json_obj.toJSONString();

        String request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;

        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);
        JSONObject response_body_map = response.getBody();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(((Map) response_body_map.get(Constants.Tokens.TWEETS)).size() == 1);
        Assert.assertTrue(((Map<String, String>) response_body_map.get(Constants.Tokens.TWEETS)).entrySet().iterator().next().getKey().equals("this is a #tweet"));
        Assert.assertTrue(response_body_map.get(Constants.Tokens.QUERY).equals(Constants.Tokens.TWEET));
        Assert.assertTrue(response_body_map.get(Constants.Tokens.VERSIONNUM).equals("1"));
    }

    @Test
    public void searchDataStoreWithUnknownQueryAndV1ShouldReturnOK() throws ParseException {

        setupServer();

        String query = "?q=bla&v=1";
        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        String timestamp_json = timestamp_json_obj.toJSONString();

        String request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;

        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);
        JSONObject response_body_map = response.getBody();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(((Map) response_body_map.get(Constants.Tokens.TWEETS)).isEmpty());
        Assert.assertTrue(response_body_map.get(Constants.Tokens.QUERY).equals("bla"));
        Assert.assertTrue(response_body_map.get(Constants.Tokens.VERSIONNUM).equals("0"));
    }

    @Test
    public void searchDataStoreWithUnknownQueryAndV0ShouldReturnNotModified() throws ParseException {

        setupServer();

        String query = "?q=bla&v=0";
        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        String timestamp_json = timestamp_json_obj.toJSONString();

        String request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;

        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.NOTMODIFIED));
    }

    @Test
    public void requestSnapshotWithoutBoundShouldReturnOK() {

        setupServer();

        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.SNAPSHOT, null, null).toString();
        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(((JSONObject) response.getBody().get(Constants.Tokens.SNAPSHOT)).size() > 1);
    }

    @Test
    public void requestSnapshotWithCongruentTimestampShouldReturnOK() {

        setupServer();

        JSONObject request_body_json = new JSONObject();
        request_body_json.put(Constants.Tokens.TIMESTAMP, 1);
        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.SNAPSHOT, null, request_body_json).toString();
        HTTPObject response = SystemUtility.sendRequest(request, data_store_socket1a);

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(((JSONObject) response.getBody().get(Constants.Tokens.SNAPSHOT)).size() > 1);
    }

    @Test
    public void requestSnapshotWithFutureTimestampShouldReturnOK() {

        setupServer();

        JSONObject snapshot_request_body_json = new JSONObject();
        snapshot_request_body_json.put(Constants.Tokens.TIMESTAMP, 2);

        // Start up thread to send replication request to data store server
        TestServerThread data_store_listener_thread = new TestServerThread(null, null, data_store_socket1b);
        JSONObject replication_json = new JSONObject();
        String tweet = "replicated tweet";
        replication_json.put(Constants.Tokens.TWEET, tweet);
        replication_json.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        replication_json.put(Constants.Tokens.SERVERID, "datastore2");
        JSONArray hash_array = new JSONArray();
        hash_array.add("tweet");
        replication_json.put(Constants.Tokens.HASH, hash_array);
        HTTPObject front_end_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                Constants.Tokens.REPLICATE, null, replication_json);

        data_store_listener_thread.setRequest(front_end_request);
        data_store_listener_thread.start();

        String snapshot_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.SNAPSHOT, null, snapshot_request_body_json).toString();
        HTTPObject snapshot_response = SystemUtility.sendRequest(snapshot_request, data_store_socket1a);

        waitBetweenTests();

        Assert.assertTrue(snapshot_response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(((JSONObject) snapshot_response.getBody().get(Constants.Tokens.SNAPSHOT)).size() > 1);
    }

    private static void waitBetweenTests() {

        synchronized (DataStore.class) {

            try {

                DataStore.class.wait(500);
            } catch (InterruptedException e) {

                e.printStackTrace();
            }
        }
    }
}
