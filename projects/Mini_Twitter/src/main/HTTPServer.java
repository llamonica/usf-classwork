package main;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple HTTP server class for MiniTwitter project. The server can be
 * deployed as either a front end server or data store server.
 *
 * Created by luke on 9/3/14.
 */
public class HTTPServer {

    private String layer;
    private String instance_id;
    private String server_id;
    private boolean development;
    private String[] discovery_server_location;
    private int testing_delay;
    private HeartBeatCounter heart_beat_counter;

    final static Logger logger = Logger.getLogger(HTTPServer.class);

    /**
     * Simple HTTP server class for MiniTwitter project.
     *
     * @param layer System layer
     * @param instance_id Numerical instance of system layer entity
     * @param development Flag for choosing port set
     * @param delay
     */
    public HTTPServer(String layer, String instance_id, boolean development, String delay) {

        this.layer = layer;
        this.instance_id = instance_id;
        this.development = development;

        if (this.layer.equals(Constants.Config.DISCOVERY)) {

            this.server_id = this.layer;
        } else {

            this.server_id = this.layer + this.instance_id;
            discovery_server_location = SystemUtility.getServerConfig(Constants.Config.DISCOVERY,
                    null,
                    this.development);

            logger.debug("Registering new server: " + this.server_id);
            String[] add_and_port = SystemUtility.getServerConfig(this.layer, this.instance_id, this.development);
            SystemUtility.registerWithDiscoveryServer(this.server_id, add_and_port, this.layer, discovery_server_location);

            // Set a replication delay for demonstration purposes only
            if (delay != null) {

                this.testing_delay = Integer.decode(delay);
            } else {

                this.testing_delay = 0;
            }
        }

        // Start a heart beat counter for testing the discovery server
        if (development) {

            heart_beat_counter = new HeartBeatCounter();
        }
    }

    /**
     * Returns the number of heart beat monitor requests received. This method
     * is meant to be used for testing the discovery server.
     *
     * @return Number of heart beat monitor requests received
     */
    public int getHeartBeatCount() {

        return heart_beat_counter.getCount();
    }

    /**
     * Commences a loop for accepting and serving HTTP requests.
     */
    public void serve() {

        ServerSocket server_socket = null;
        int port = -1;

        try {

            // Retrieve the port from which the server will receive connections
            port = Integer.decode(SystemUtility.getServerConfig(this.layer, this.instance_id, this.development)[1]);
            server_socket = new ServerSocket(port);
        } catch (IOException e) {

            logger.error("Unable to initialize server socket on port: " + port);
            System.exit(1);
        }

        ExecutorService executor = Executors.newFixedThreadPool(10);

        while (true) {

            Socket socket;

            try {

                // Accept the next socket connection
                socket = server_socket.accept();
                logger.debug("Serving new incoming signal on port: " + socket.getPort());

                // Create a worker thread for the defined system layer
                if (this.layer.equals(Constants.Config.DATASTORE)) {

                    logger.debug("Executing new DataStoreWorker");
                    executor.execute(new DataStoreWorker(socket, heart_beat_counter, this.server_id, this.development, this.testing_delay));
                } else if (this.layer.equals(Constants.Config.FRONTEND)) {

                    logger.debug("Executing new FrontEndWorker");
                    executor.execute(new FrontEndWorker(socket, this.server_id, this.development));
                } else if (this.layer.equals(Constants.Config.DISCOVERY)) {

                    logger.debug("Executing new DiscoveryWorker");
                    executor.execute(new DiscoveryWorker(socket, this.server_id, this.development));
                }
            } catch (IOException e) {

                // Unable to accept the socket connection, so log and skip it
                logger.error("Server socket failed");
                continue;
            }
        }
    }

    public static void main(String[] args) {

        HTTPServer httpServer = null;
        if (args.length == 3) {

            httpServer = new HTTPServer(args[0], args[1], Boolean.parseBoolean(args[2]), null);
        } else if (args.length == 4) {

            httpServer = new HTTPServer(args[0], args[1], Boolean.parseBoolean(args[2]), args[3]);
        } else {

            System.out.println("Usage: java HTTPServer <layer> <instance> <dev_flag> <delay (optional)>");
            System.exit(1);
        }

        httpServer.serve();
    }
}