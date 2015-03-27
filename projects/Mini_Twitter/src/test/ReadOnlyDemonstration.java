package test;

import main.*;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * A program for testing and demonstrating the lock-free snapshot
 * functionality.
 *
 * Created by luke on 12/9/14.
 */
public class ReadOnlyDemonstration {

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length != 1) {

            System.out.println("Usage: java ReadOnlyDemonstration <development_flag>");
        }

        //TODO: Launch three data store servers (one with large delay) and three front end servers
        /*
            java -cp lib/*:bin/:src main.HTTPServer discovery 0 false
            java -cp lib/*:bin/:src main.HTTPServer datastore 1 false
            java -cp lib/*:bin/:src main.HTTPServer datastore 2 false
            java -cp lib/*:bin/:src main.HTTPServer datastore 3 false 10000
            java -cp lib/*:bin/:src main.HTTPServer frontend 1 false
            java -cp lib/*:bin/:src main.HTTPServer frontend 2 false
            java -cp lib/*:bin/:src main.HTTPServer frontend 3 false

            or

            java -cp lib/*:bin/:src main.HTTPServer discovery 0 true
            java -cp lib/*:bin/:src main.HTTPServer datastore 1 true
            java -cp lib/*:bin/:src main.HTTPServer datastore 2 true
            java -cp lib/*:bin/:src main.HTTPServer datastore 3 true 10000
            java -cp lib/*:bin/:src main.HTTPServer frontend 1 true
            java -cp lib/*:bin/:src main.HTTPServer frontend 2 true
            java -cp lib/*:bin/:src main.HTTPServer frontend 3 true
         */

        // Initialize lists of addresses, ports, and IDs
        ArrayList<String> datastore_addresses = new ArrayList<String>();
        ArrayList<String> datastore_ids = new ArrayList<String>();
        ArrayList<Integer> datastore_ports = new ArrayList<Integer>();

        ArrayList<String> frontend_addresses = new ArrayList<String>();
        ArrayList<String> frontend_ids = new ArrayList<String>();
        ArrayList<Integer> frontend_ports = new ArrayList<Integer>();

        for (int i = 1; i <= 4; i++) {

            String[] datastore_params = SystemUtility.getServerConfig(Constants.Config.DATASTORE, String.valueOf(i),
                    Boolean.parseBoolean(args[0]));

            datastore_addresses.add(datastore_params[0]);
            datastore_ports.add(Integer.decode(datastore_params[1]));
            datastore_ids.add(Constants.Config.DATASTORE + i);
        }

        for (int i = 1; i <= 3; i++) {

            String[] frontend_params = SystemUtility.getServerConfig(Constants.Config.FRONTEND, String.valueOf(i),
                    Boolean.parseBoolean(args[0]));

            frontend_addresses.add(frontend_params[0]);
            frontend_ports.add(Integer.decode(frontend_params[1]));
            frontend_ids.add(Constants.Config.FRONTEND + i);
        }

        // Declare some useful variables
        int tweet_count = 0;
        HashMap<String, String> client_query_map;
        JSONObject request_body;
        HTTPObject request;
        Socket client_socket;
        HTTPObject response;
        Scanner scanner = new Scanner(System.in);

        // Post three tweets, one to each server
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating tweet posting.");
        System.out.println("*******************************************************************************\n\n");
        for (int i = 0; i < 3; i++) {

            request_body = new JSONObject();
            request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
            request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                    request_body);
            System.out.println("\nSending request:\n" + request.toString());
            client_socket = new Socket(frontend_addresses.get(i), frontend_ports.get(i));
            System.out.println("...to " + frontend_addresses.get(i) + ":" + frontend_ports.get(i));
            response = SystemUtility.sendRequest(request.toString(), client_socket);
            System.out.println("\nPress ENTER to continue...");
            scanner.nextLine();
        }

        // Send search query request
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating tweet searching.");
        System.out.println("*******************************************************************************\n\n");
        client_query_map = new HashMap<String, String>();
        client_query_map.put(Constants.Tokens.QUERY, Constants.Tokens.TWEET);
        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.TWEETS, client_query_map,
                null);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();

        // Post three tweets, one to each server
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating tweet posting.");
        System.out.println("*******************************************************************************\n\n");
        for (int i = 0; i < 3; i++) {

            request_body = new JSONObject();
            request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
            request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                    request_body);
            System.out.println("\nSending request:\n" + request.toString());
            client_socket = new Socket(frontend_addresses.get(i), frontend_ports.get(i));
            System.out.println("...to " + frontend_addresses.get(i) + ":" + frontend_ports.get(i));
            response = SystemUtility.sendRequest(request.toString(), client_socket);
            System.out.println("\nPress ENTER to continue...");
            scanner.nextLine();
        }

        // Send search query request
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating tweet searching.");
        System.out.println("*******************************************************************************\n\n");
        client_query_map = new HashMap<String, String>();
        client_query_map.put(Constants.Tokens.QUERY, Constants.Tokens.TWEET);
        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.TWEETS, client_query_map,
                null);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();

        // Post three tweets, each to different servers
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating tweet posting.");
        System.out.println("*******************************************************************************\n\n");
        for (int i = 0; i < 3; i++) {

            request_body = new JSONObject();
            request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
            request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                    request_body);
            System.out.println("\nSending request:\n" + request.toString());
            client_socket = new Socket(frontend_addresses.get(i), frontend_ports.get(i));
            System.out.println("...to " + frontend_addresses.get(i) + ":" + frontend_ports.get(i));
            response = SystemUtility.sendRequest(request.toString(), client_socket);
            System.out.println("\nPress ENTER to continue...");
            scanner.nextLine();
        }

        // Send last tweet from FE1 to BE3 to capture timestamp that will require search blocking at BE1
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating out-dating a server.");
        System.out.println("*******************************************************************************\n\n");
        request_body = new JSONObject();
        request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
        request_body.put(Constants.Tokens.SERVERID, "datastore3");
        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                request_body);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        System.out.println("...to " + frontend_addresses.get(0) + ":" + frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();

        // Send search query request with future timestamp
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating searching an outdated server.");
        System.out.println("*******************************************************************************\n\n");
        client_query_map = new HashMap<String, String>();
        client_query_map.put(Constants.Tokens.QUERY, Constants.Tokens.TWEET);
        request_body = null;
        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.TWEETS, client_query_map,
                request_body);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();

        // Demonstrate snapshot without delay
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating snapshot request.");
        System.out.println("*******************************************************************************\n\n");

        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.SNAPSHOT, null, null);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();

        // Demonstrate snapshot with delay and retry
        System.out.println("\n\n*******************************************************************************");
        System.out.println("Demonstrating snapshot requests wait until timestamps agree.");
        System.out.println("*******************************************************************************\n\n");

        CatchUpThread catch_up_thread = new CatchUpThread(frontend_addresses, frontend_ports, tweet_count);
        catch_up_thread.start();

        // First send snapshot request
        request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET, Constants.Tokens.SNAPSHOT, null, null);
        System.out.println("\nSending request:\n" + request.toString());
        client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
        response = SystemUtility.sendRequest(request.toString(), client_socket);

        //TODO: wait for snapshot to return
        System.out.println("\nPress ENTER to continue...");
        scanner.nextLine();
    }

    public static class CatchUpThread extends Thread {

        public ArrayList<String> frontend_addresses;
        public ArrayList<Integer> frontend_ports;
        public Integer tweet_count;

        public CatchUpThread(ArrayList<String> frontend_addresses, ArrayList<Integer> frontend_ports, Integer tweet_count) {

            this.frontend_addresses = frontend_addresses;
            this.frontend_ports = frontend_ports;
            this.tweet_count = tweet_count;
        }

        public void run() {

            // Post two tweets, each to different servers
            System.out.println("\n\n*******************************************************************************");
            System.out.println("Demonstrating catching up the outdated server.");
            System.out.println("*******************************************************************************\n\n");

            synchronized (CatchUpThread.class) {

                try {

                    System.out.println("Waiting " + Constants.Discovery.TIMEOUT / 5000.0 + " seconds before sending tweets to server");
                    CatchUpThread.class.wait(Constants.Discovery.TIMEOUT / 5);
                } catch (InterruptedException ignored) { }
            }

            JSONObject request_body = new JSONObject();
            request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
            HTTPObject request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                    request_body);
            System.out.println("\nSending request:\n" + request.toString());
            Socket client_socket = null;
            try {
                client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("...to " + frontend_addresses.get(0) + ":" + frontend_ports.get(0));
            HTTPObject response = SystemUtility.sendRequest(request.toString(), client_socket);

            request_body = new JSONObject();
            request_body.put(Constants.Tokens.TWEET, "this is #tweet number " + ++tweet_count);
            request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST, Constants.Tokens.TWEETS, null,
                    request_body);
            System.out.println("\nSending request:\n" + request.toString());
            try {
                client_socket = new Socket(frontend_addresses.get(0), frontend_ports.get(0));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("...to " + frontend_addresses.get(0) + ":" + frontend_ports.get(0));
            response = SystemUtility.sendRequest(request.toString(), client_socket);

        }
    }
}
