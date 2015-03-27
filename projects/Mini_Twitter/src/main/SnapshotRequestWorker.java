package main;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

/**
 * Thread class used to request a snapshot from a data store server.
 *
 * Created by luke on 12/8/14.
 */
public class SnapshotRequestWorker extends Thread {

    private String server_location;
    private String request;
    private HTTPObject response;
    private String server_id;

    final static Logger logger = Logger.getLogger(ReplicationBroadcastWorker.class);

    public SnapshotRequestWorker(String server_location, String request, String server_id) {

        this.server_location = server_location;
        this.request = request;
        this.response = null;
        this.server_id = server_id;
    }

    /**
     * Sends a HTTP request to retrieve a snapshot from the destination server.
     */
    @Override
    public void run() {

        try {

            String[] add_and_port = server_location.split(":");
            Socket replication_socket = new Socket(add_and_port[0], Integer.decode(add_and_port[1]));
            response = SystemUtility.sendRequest(request, replication_socket);
        } catch (IOException e) {

            logger.error("Unable to initialize server socket for location: " + server_location);
        }
    }

    /**
     * Returns the response to the sent request if one exists.
     *
     * @return HTTPObject response
     */
    public HTTPObject getResponse() {

        return response;
    }

    /**
     * Returns the destination server ID.
     *
     * @return String destination server ID
     */
    public String getServerID() {

        return server_id;
    }
}
