package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * Thread worker class for discovery server. Registers servers with the server
 * directory master, broadcasts copies of the directory, and manages heart
 * beat monitoring of data store servers.
 *
 * Created by luke on 10/29/14.
 */
public class DiscoveryWorker extends ServerWorkerThread {

    private static HeartBeatMonitor heart_beat_monitor = null;

    final static Logger logger = Logger.getLogger(DiscoveryWorker.class);

    public DiscoveryWorker(Socket in_socket, String server_id, boolean development) {

        super(in_socket, server_id, development);
        if (heart_beat_monitor == null) {

            heart_beat_monitor = HeartBeatMonitor.getInstance();
            server_directory = heart_beat_monitor.server_directory;
        }

        if (development || this.valid_paths == null) {

            this.valid_paths = new HashSet<String>();
            this.valid_paths.add("/" + Constants.Tokens.DISCOVER);
        }
    }

    /**
     * Validates the URI path and uses the HTTP method to choose the proper
     * processing functionality.
     */
    @Override
    public void processRequest() {

        if (!validatePathAndMethod()) {

            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

            logger.debug("Retrieving server from directory master");
            sendServerDirectory();
        } else if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

            logger.debug("Adding server to directory master");
            addServerToDirectory((String) request_body.get(Constants.Tokens.SERVERID),
                                (String) request_body.get(Constants.Tokens.SERVERLOC));
        }
    }

    /**
     * Builds a response to requests for a copy of the server directory master.
     */
    private void sendServerDirectory() {

        JSONObject server_json_mapping = heart_beat_monitor.server_directory.toJSONObject();
        response_body = new JSONObject();
        response_body.put(Constants.Tokens.STATUS, Constants.Messages.OK);
        response_body.put(Constants.Discovery.SERVERS, server_json_mapping);
        response = SystemUtility.buildResponse(Constants.Codes.OK, response_body);
        response_code = Constants.Codes.OK;
    }

    /**
     * Updates the server directory master with the given server id and its
     * location. If the given server is a data store server, it is registered
     * with the heart beat monitor. Finally, broadcasts the updated server
     * directory master to all active, registered servers.
     *
     * @param id
     * @param location
     */
    private void addServerToDirectory(String id, String location) {

        heart_beat_monitor.server_directory.addServer(id, location);
        logger.debug("Updated server directory master:\n" + heart_beat_monitor.server_directory.toJSONObject().toJSONString());

        // If new server is a DataStore, then register it for heart beat monitoring
        if (request_body.get(Constants.Tokens.LAYER).equals(Constants.Config.DATASTORE)) {

            heart_beat_monitor.addMonitor(id, location);
        }

        try {

            // Attempt to close the input socket so a connection can be made with the new server for the broadcast
            input_socket.close();
        } catch (IOException ignored) { }

        // Broadcast new directory mapping
        workers = new ArrayList<Thread>();
        HashMap<String, String> server_map = heart_beat_monitor.server_directory.getServerCollection();
        for (Map.Entry<String, String> entry : server_map.entrySet()) {

            location = entry.getValue();
            if (location != null) {
                
                DiscoveryBroadcastWorker dbw = new DiscoveryBroadcastWorker(location, heart_beat_monitor.server_directory);
                workers.add(dbw);
                dbw.start();
            }
        }
    }

    /**
     * A singleton class for monitoring the health of all data store servers
     * registered with the discovery server.
     */
    private static class HeartBeatMonitor {

        private static HeartBeatMonitor heart_beat_monitor = null;
        private static ServerDirectory server_directory;
        private static HashSet<String> active_monitors;

        final static Logger logger = Logger.getLogger(HeartBeatMonitor.class);

        public static HeartBeatMonitor getInstance() {

            logger.debug("Instantiating new heart beat monitor");
            if (heart_beat_monitor == null) {

                server_directory = ServerDirectory.getInstance(null, false);
                heart_beat_monitor = new HeartBeatMonitor(server_directory);
            }

            return heart_beat_monitor;
        }

        private HeartBeatMonitor(ServerDirectory server_directory) {

            this.server_directory = server_directory;
            active_monitors = new HashSet<String>();
        }

        /**
         * Adds a server to the server directory master, and starts a heart
         * beat monitoring thread if one is not already active for the given
         * server.
         *
         * @param id String ID of the new server
         * @param location Location of the new server
         */
        private void addMonitor(String id, String location) {

            server_directory.addServer(id, location);
            startMonitor(id, location);
        }

        /**
         * Starts the monitoring of the given server if it is not already
         * being monitored by a DiscoveryHeartbeatWorker.
         *
         * @param id String ID of the new server
         * @param location Location of the new server
         */
        private void startMonitor(String id, String location) {

            // Verify location is not already being monitored
            if (!active_monitors.contains(location)) {

                logger.debug("Starting heart beat monitor for server ID: " + id);
                int timeout = Constants.Discovery.TIMEOUT;
                new DiscoveryHeartbeatWorker(server_directory, timeout, id).start();
                active_monitors.add(location);
            }
        }
    }
}
