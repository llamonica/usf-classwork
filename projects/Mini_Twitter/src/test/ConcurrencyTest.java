package test;

import main.Constants;
import main.HTTPConstants;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by luke on 10/1/14.
 */
public class ConcurrencyTest {

    private static String client_tweet = "{\"tweet\": \"this is an example #exampleTweet\"}";
    private static String example_post = HTTPConstants.HTTPMethod.POST + " " +
            "/" + Constants.Tokens.TWEETS + " " +
            Constants.Tokens.HTTPVERSION + "\n" +
            "content-length:" + client_tweet.getBytes().length + "\n\n" + client_tweet;

    private static String example_search = HTTPConstants.HTTPMethod.GET + " " +
            "/" + Constants.Tokens.TWEETS + "?q=tweet " +
            Constants.Tokens.HTTPVERSION + "\n" +
            "content-length:" + client_tweet.getBytes().length + "\n\n";

    final static Logger logger = Logger.getLogger(ConcurrencyTest.class);

    public ConcurrencyTest() {

    }

    public void writeMultipleConcurrentPosts() {

        // Read and parse the JSON configuration map from file
        String json_string = "";
        try {

            File f = new File("./server_config.json");
            Scanner fscanner = new Scanner(new FileReader(f));
            while (fscanner.hasNext()) {

                json_string += fscanner.nextLine();
            }

        } catch (FileNotFoundException e) {

            logger.error("Unable to find server configuration file");
            System.exit(1);
        }

        JSONObject json_map = (JSONObject) JSONValue.parse(json_string);
        json_map = (JSONObject) json_map.get(Constants.Config.PRODUCTION);

        JSONObject server_port_map = ((JSONObject) json_map.get(Constants.Config.PORTS));

        TestConcurrencyThread[] threads = new TestConcurrencyThread[3];
        for (int i = 0; i < 3; i++) {

            String ip = (String) json_map.get(Constants.Config.IP + (i + 1));
            String port = (String) server_port_map.get(Constants.Config.FRONTEND + (i + 1));
            threads[i] = new TestConcurrencyThread(1000, ip, port, example_post);
            logger.debug("Created thread to send posts to ip=" + ip + ", port=" + port);
        }

        for (int i = 0; i < 3; i++) {

            threads[i].run();
        }
    }

    public void writeMultipleConcurrentSearches() {

        // Read and parse the JSON configuration map from file
        String json_string = "";
        try {

            File f = new File("./server_config.json");
            Scanner fscanner = new Scanner(new FileReader(f));
            while (fscanner.hasNext()) {

                json_string += fscanner.nextLine();
            }

        } catch (FileNotFoundException e) {

            logger.error("Unable to find server configuration file");
            System.exit(1);
        }

        JSONObject json_map = (JSONObject) JSONValue.parse(json_string);
        json_map = (JSONObject) json_map.get(Constants.Config.PRODUCTION);

        JSONObject server_port_map = ((JSONObject) json_map.get(Constants.Config.PORTS));

        TestConcurrencyThread[] threads = new TestConcurrencyThread[3];
        for (int i = 0; i < 3; i++) {

            String ip = (String) json_map.get(Constants.Config.IP + (i + 1));
            String port = (String) server_port_map.get(Constants.Config.FRONTEND + (i + 1));
            threads[i] = new TestConcurrencyThread(20, ip, port, example_search);
            logger.debug("Created thread to send posts to ip=" + ip + ", port=" + port);

            threads[i].start();
        }

        for (int i = 0; i < 3; i++) {

        }
    }

    private class TestConcurrencyThread extends Thread {

        private int num_requests;
        private String ip;
        private String port;
        private String request;

        public TestConcurrencyThread(int num_requests, String ip, String port, String request) {

            this.num_requests = num_requests;
            this.ip = ip;
            this.port = port;
            this.request = request;
        }

        @Override
        public void run() {

            for (int i = 0; i < num_requests; i++) {

                try {

                    Socket socket = new Socket(ip, Integer.decode(port));
                    OutputStream out = socket.getOutputStream();
                    out.write(request.getBytes());
                    out.flush();

                    socket.close();
                } catch (IOException e) {

                    logger.error("Unable to open socket");
                }
            }
        }
    }

    public static void main(String[] args) {

        ConcurrencyTest test = new ConcurrencyTest();

        if (args[0].equals("posts")) {

            test.writeMultipleConcurrentPosts();
        } else if (args[0].equals("searches")) {

            test.writeMultipleConcurrentSearches();
        }
    }
}
