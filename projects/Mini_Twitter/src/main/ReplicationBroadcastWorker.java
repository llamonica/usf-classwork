package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;

/**
 * Thread class used for broadcasting a data write to a single replica.
 *
 * Created by luke on 11/2/14.
 */
public class ReplicationBroadcastWorker extends Thread {

    private String server_location;
    private String request;
    private int testing_delay;

    final static Logger logger = Logger.getLogger(ReplicationBroadcastWorker.class);

    public ReplicationBroadcastWorker(String server_location, String request, int delay) {

        this.server_location = server_location;
        this.request = request;
        this.testing_delay = delay;
    }

    /**
     * Sends a HTTP request with a copy of the incoming data write to a given
     * server.
     */
    @Override
    public void run() {

        // Force a replication delay for demonstrating proper blocking of outdated searches
        if (testing_delay > 0) {

            synchronized (this) {

                try {

                    this.wait(testing_delay);
                } catch (InterruptedException e) {

                    logger.error(Constants.Messages.INTERRUPTED);
                }
            }
        }

        try {

            String[] add_and_port = server_location.split(":");
            Socket replication_socket = new Socket(add_and_port[0], Integer.decode(add_and_port[1]));
            SystemUtility.sendRequestWithoutWait(request, replication_socket);
        } catch (IOException e) {

            logger.error("Unable to initialize server socket for location: " + server_location);
        }
    }
}
