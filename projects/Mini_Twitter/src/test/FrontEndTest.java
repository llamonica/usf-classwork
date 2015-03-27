package test;

import main.*;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by luke on 9/27/14.
 */
public class FrontEndTest {

    private static boolean servers_active = false;
    private static HTTPServer discovery_server;
    private static HTTPServer front_end_server;
    private static String discovery_ip;
    private static int discovery_port;
    private static String front_end_ip;
    private static int front_end_port;
    private static Socket client_socket;
    private static ServerSocket data_store_server_socket1;
    private static ServerSocket data_store_server_socket2;
    private static OutputStream front_end_out;
    private static BufferedReader front_end_in;
    private static Socket discovery_socket;
    private static OutputStream discovery_out;
    private static BufferedReader discovery_in;
    private static TestServerThread front_end_server_thread;
    private static TestServerThread discovery_server_thread;
    private static String[] ds_server_params1;
    private static String[] ds_server_params2;
    private static String[] discovery_params;
    private static String[] fe_server_params;
    private static VectorTimestamp vector_timestamp;

    final static Logger logger = Logger.getLogger(FrontEndTest.class);

    private static void setupServer() {

        try {

            if (!servers_active) {

                ds_server_params1 = SystemUtility.getServerConfig(Constants.Config.DATASTORE, "1", true);
                ds_server_params2 = SystemUtility.getServerConfig(Constants.Config.DATASTORE, "2", true);

                discovery_params = SystemUtility.getServerConfig(Constants.Config.DISCOVERY, null, true);

                discovery_ip = discovery_params[0];
                discovery_port = Integer.decode(discovery_params[1]);

                // Initialize one discovery server thread and one front end server thread
                discovery_server = new HTTPServer(Constants.Config.DISCOVERY, null, true, null);

                discovery_server_thread = new TestServerThread(discovery_server, null, null);
                discovery_server_thread.start();

                waitBetweenTests();
                waitBetweenTests();

                front_end_server = new HTTPServer(Constants.Config.FRONTEND, "1", true, null);

                front_end_server_thread = new TestServerThread(front_end_server, null, null);
                front_end_server_thread.start();

                waitBetweenTests();
                waitBetweenTests();

                fe_server_params = SystemUtility.getServerConfig(Constants.Config.FRONTEND, "1", true);
                front_end_ip = fe_server_params[0];
                front_end_port = Integer.decode(fe_server_params[1]);

                servers_active = true;
            }

            data_store_server_socket1 = new ServerSocket(Integer.decode(ds_server_params1[1]));

            client_socket = new Socket(front_end_ip, front_end_port);
            front_end_out = client_socket.getOutputStream();
            front_end_in = new BufferedReader(new InputStreamReader(client_socket.getInputStream()));

            discovery_socket = new Socket(discovery_ip, discovery_port);
            discovery_out = discovery_socket.getOutputStream();
            discovery_in = new BufferedReader(new InputStreamReader(discovery_socket.getInputStream()));

            HashMap<String, String> timestamp_map = new HashMap<String, String>();
            timestamp_map.put("datastore1", "1");
            timestamp_map.put("datastore2", "1");
            timestamp_map.put("frontend1", "0");
            vector_timestamp = new VectorTimestamp(timestamp_map);
        } catch (IOException e) {

            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void postBasicTweetShouldReturnCreated() throws InterruptedException, IOException, ParseException {

        setupServer();

        String client_tweet = "{\"tweet\": \"this is a #tweet\"}";
        JSONObject client_tweet_body = (JSONObject) new JSONParser().parse(client_tweet);

        String front_end_target_request = "POST /tweets HTTP/1.1\n" +
                "content-length:49\n\n{\"tweet\":\"this is a #tweet\",\"hashtags\":[\"tweet\"]}";

        JSONObject data_store_response_body = new JSONObject();
        data_store_response_body.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        HTTPObject data_store_response = SystemUtility.buildResponse(Constants.Codes.CREATED, data_store_response_body);

        String client_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                Constants.Tokens.TWEETS, null, client_tweet_body).toString();

        TestServerThread data_store_listener_thread = new TestServerThread(null, data_store_server_socket1, null);

        data_store_listener_thread.setResponse(data_store_response);
        data_store_listener_thread.start();
        logger.debug("Started thread to listen on port: " + data_store_server_socket1.getLocalPort());

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        String front_end_actual_request = data_store_listener_thread.getInput();
        logger.debug("Request sent from front end: " + front_end_actual_request);
        logger.debug("Response sent to client: " + response);

        data_store_listener_thread.join();
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.CREATED));
        Assert.assertTrue(front_end_actual_request.equals(front_end_target_request));
    }

    @Test
    public void postTweetWithNoHashtagShouldReturnBadRequest() throws InterruptedException, IOException {

        setupServer();

        String client_tweet = "{\"tweet\": \"this is a bad tweet\"}";

        String client_request = HTTPConstants.HTTPMethod.POST + " " +
                "/" + Constants.Tokens.TWEETS + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + client_tweet.getBytes().length + "\n\n" + client_tweet;

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        logger.debug("Response sent to client: " + response);
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.BADREQUEST));
    }

    @Test
    public void postTweetWithIncompleteHashtagShouldReturnBadRequest() throws InterruptedException, IOException {

        setupServer();

        String client_tweet = "{\"tweet\": \"this is a bad # tweet\"}";

        String client_request = HTTPConstants.HTTPMethod.POST + " " +
                "/" + Constants.Tokens.TWEETS + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + client_tweet.getBytes().length + "\n\n" + client_tweet;

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        logger.debug("Response sent to client: " + response);
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.BADREQUEST));
    }

    @Test
    public void postTweetWithBadEndpointShouldReturnBadRequest() throws InterruptedException, IOException {

        setupServer();

        String client_tweet = "{\"tweet\": \"this is a #tweet\"}";

        String client_request = HTTPConstants.HTTPMethod.POST + " " +
                "/nowhere " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + client_tweet.getBytes().length + "\n\n" + client_tweet;

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        logger.debug("Response sent to client: " + response);
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.NOTFOUND));
    }

    @Test
    public void searchDataStoreWithValidVersionShouldReturnOK() throws InterruptedException, IOException, ParseException {

        setupServer();

        CacheStore cache = CacheStore.getInstance();
        cache.writeCache("tweet", new JSONObject(), "1");

        String front_end_query1 = "?q=tweet&v=1";
        String front_end_query2 = "?v=1&q=tweet";
        String client_query = "?q=tweet";

        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, vector_timestamp.toJSONObject());
        String timestamp_json = timestamp_json_obj.toJSONString();

        String front_end_target_request1 = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + front_end_query1 + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;
        String front_end_target_request2 = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + front_end_query2 + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;
        String client_request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + client_query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + 0 + "\n\n";

        HTTPObject data_store_response = SystemUtility.buildResponse(Constants.Codes.NOTMODIFIED, vector_timestamp.toJSONObject());

        TestServerThread data_store_listener_thread = new TestServerThread(null, data_store_server_socket1, null);

        data_store_listener_thread.setResponse(data_store_response);
        data_store_listener_thread.start();
        logger.debug("Started thread to listen on port: " + data_store_server_socket1.getLocalPort());

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        String front_end_actual_request = data_store_listener_thread.getInput();
        logger.debug("Request sent from front end: " + front_end_actual_request);
        logger.debug("Response sent to client: " + response);

        data_store_listener_thread.join();
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(front_end_actual_request.trim().equals(front_end_target_request1.trim()) ||
                        front_end_actual_request.trim().equals(front_end_target_request2.trim()));
    }

    @Test
    public void searchDataStoreWithInvalidVersionShouldReturnOK() throws InterruptedException, IOException, ParseException {

        setupServer();

        CacheStore cache = CacheStore.getInstance();
        JSONObject tweets = new JSONObject();
        tweets.put("tw1", vector_timestamp.toJSONObject());
        tweets.put("tw2", vector_timestamp.toJSONObject());

        cache.writeCache("notTweet", tweets, "1");

        String client_query = "?q=notTweet";

        JSONObject timestamp_json_obj = new JSONObject();
        timestamp_json_obj.put(Constants.Tokens.TIMESTAMP, new VectorTimestamp(new HashMap<String, String>()));
        String timestamp_json = timestamp_json_obj.toJSONString();

        String client_request = HTTPConstants.HTTPMethod.GET + " " +
                "/" + Constants.Tokens.TWEETS + client_query + " " +
                Constants.Tokens.HTTPVERSION + "\n" +
                "content-length:" + timestamp_json.length() + "\n\n" + timestamp_json;

        HTTPObject data_store_response = SystemUtility.buildResponse(Constants.Codes.OK,
                (JSONObject) new JSONParser().parse("{\"q\":\"notTweet\",\"v\":\"2\"," +
                        "\"tweets\":{\"tweet_message1\":{\"frontend1\":\"0\",\"datastore1\":\"1\"}," +
                        "\"tweet_message2\":{\"frontend1\":\"1\",\"datastore1\":\"1\"}}}"));
        HTTPObject expected_response = SystemUtility.buildResponse(Constants.Codes.OK,
                (JSONObject) new JSONParser().parse("{\"q\":\"notTweet\",\"tweets\":[\"tweet_message1\",\"tweet_message2\"]}"));

        TestServerThread data_store_listener_thread = new TestServerThread(null, data_store_server_socket1, null);

        data_store_listener_thread.setResponse(data_store_response);
        data_store_listener_thread.start();
        logger.debug("Started thread to listen on port: " + data_store_server_socket1.getLocalPort());

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        String front_end_actual_request = data_store_listener_thread.getInput();
        logger.debug("Request sent from front end: " + front_end_actual_request);
        logger.debug("Response sent to client: " + response);

        data_store_listener_thread.join();
        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.OK));
        Assert.assertTrue(response.toString().equals(expected_response.toString()));
    }

    @Test
    public void searchDataStoreWithSpaceInQueryShouldReturnBadRequest() throws InterruptedException, IOException {

        setupServer();

        HashMap<String, String> client_query_map = new HashMap<String, String>();
        client_query_map.put("q", "not Tweet");

        JSONObject timestamp_json = new JSONObject();
        timestamp_json.put(Constants.Tokens.TIMESTAMP, new VectorTimestamp(new HashMap<String, String>()));

        String client_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.TWEETS,
                client_query_map, timestamp_json).toString();

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        logger.debug("Response sent to client: " + response);

        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.BADREQUEST));
    }

    @Test
    public void searchDataStoreNoQueryTermShouldReturnBadRequest() throws InterruptedException, IOException {

        setupServer();

        String client_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.TWEETS,
                null, null).toString();

        HTTPObject response = SystemUtility.sendRequest(client_request, client_socket);

        logger.debug("Response sent to client: " + response);

        data_store_server_socket1.close();

        waitBetweenTests();

        Assert.assertTrue(response.getStatusCode().equals(Constants.Codes.BADREQUEST));
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
