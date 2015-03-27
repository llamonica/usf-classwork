package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;

/**
 * Thread class for broadcasting a server directory to a single server
 * location.
 *
 * Created by luke on 10/31/14.
 */
public class DiscoveryBroadcastWorker extends Thread {

    private String location;
    private ServerDirectory server_directory;

    final static Logger logger = Logger.getLogger(DiscoveryBroadcastWorker.class);

    public DiscoveryBroadcastWorker(String location, ServerDirectory server_directory) {

        this.location = location;
        this.server_directory = server_directory;
    }

    /**
     * Builds and sends a HTTP request containing a copy of the server
     * directory master.
     */
    @Override
    public void run() {

        String server_ip = null;
        Integer server_port = null;
        try {

            synchronized (this) {

                this.wait(Constants.Discovery.LATENCYUPBOUND);
            }

            server_ip = location.split(":")[0];
            server_port = Integer.decode(location.split(":")[1]);

            Socket broadcast_socket = new Socket(server_ip, server_port);
            JSONObject request_body = new JSONObject();
            request_body.put(Constants.Tokens.SERVERS, this.server_directory.toJSONObject());

            HTTPObject request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                    Constants.Tokens.DISCOVER, null,
                    request_body);

            logger.debug("Sending server directory broadcast to: " + server_ip + ":" + server_port);
            SystemUtility.sendRequestWithoutWait(request.toString(), broadcast_socket);
        } catch (IOException e) {

            logger.error("Unable to initialize server socket for location: " + server_ip + ":" + server_port);
        } catch (InterruptedException e) {

            logger.error(Constants.Messages.INTERRUPTED);
        }
    }
}
