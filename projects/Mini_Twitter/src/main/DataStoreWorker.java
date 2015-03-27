package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * Worker thread for the data store server.
 *
 * Created by luke on 9/25/14.
 */
public class DataStoreWorker extends ServerWorkerThread {

    private DataStore data_store;
    private HeartBeatCounter heart_beat_counter;
    private int testing_delay;
    private static VectorTimestamp timestamp = null;

    final static Logger logger = Logger.getLogger(DataStoreWorker.class);

    public DataStoreWorker(Socket in_socket, HeartBeatCounter heart_beat_counter, String server_id,
                           boolean development, int testing_delay) {

        super(in_socket, server_id, development);
        this.data_store = DataStore.getInstance();
        this.heart_beat_counter = heart_beat_counter;
        this.testing_delay = testing_delay;

        if (development || this.valid_paths == null) {

            this.valid_paths = new HashSet<String>();
            this.valid_paths.add("/" + Constants.Tokens.TWEETS);
            this.valid_paths.add("/" + Constants.Tokens.REPLICATE);
            this.valid_paths.add("/" + Constants.Tokens.DISCOVER);
            this.valid_paths.add("/" + Constants.Tokens.SNAPSHOT);
        }

        if (timestamp == null) {

            timestamp = new VectorTimestamp(server_directory, server_id);
        }
    }

    /**
     * Validates the URI path and uses the HTTP method to choose the proper
     * processing functionality.
     */
    @Override
    protected void processRequest() {

        if (!validatePathAndMethod()) {

            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.TWEETS)) {

            if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                logger.debug("Searching data store");
                searchDataStore();
            } else if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

                logger.debug("Posting tweet to data store");
                updateDataStore();
            }
        } else if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.REPLICATE)) {

            if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                logger.debug("Replicating data store to secondary server");
                replicateDataStore();
            } else if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

                logger.debug("Replicating tweet to data store");
                replicateIncomingData();
            }
        } else if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.DISCOVER)) {

            if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                logger.debug("Sending heart beat response");
                if (development) {

                    heart_beat_counter.incrementCount();
                }
                sendHeartBeatResponse();
            } else if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

                logger.debug("Updating server directory");
                updateServerDirectory();

                if (timestamp.getTimestamp(this.server_id) == 0) {

                    logger.debug("Requesting data store catch up");
                    requestFullDataStoreUpdate();
                }
                timestamp.fillMissingValues(server_directory);
            }
        } else if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.SNAPSHOT)) {

            if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                logger.debug("Retrieving server snapshots");
                sendSnapshot();
            }
        }
    }

    /**
     * Searches the data store for a given query and prepares a HTTP response.
     */
    private void searchDataStore() {

        // Block search until timestamp is brought up to date
        VectorTimestamp comparison_timestamp = new VectorTimestamp((JSONObject) request_body.get(Constants.Tokens.TIMESTAMP));

        LinkedList<String[]> outdated_servers = timestamp.getOutdatedServerID(comparison_timestamp, this.server_id);

        for (String[] outdated_server : outdated_servers) {

            String ahead_server_id = outdated_server[0];
            String ahead_time_stamp = outdated_server[1];
            String known_time_stamp = outdated_server[2];
            String ahead_server_loc = server_directory.getServerLocation(ahead_server_id);

            // Ahead server is offline, so check to see if the missing data was replicated
            if (ahead_server_loc == null) {

                for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

                    String candidate_server_id = entry.getKey();
                    if (!candidate_server_id.equals(server_id) &&
                            !candidate_server_id.equals(ahead_server_id) &&
                            entry.getValue() != null) {

                        ahead_server_loc = entry.getValue();
                        break;
                    }
                }
                requestUpdate(ahead_server_id, ahead_time_stamp, known_time_stamp, ahead_server_loc);
            } else if (timestamp.getTimestamp(ahead_server_id) != null &&
                    timestamp.getTimestamp(server_id) >= Integer.decode(ahead_time_stamp)) {

                logger.debug("DataStore received update for " + ahead_server_id + " before waiting.");
                continue;
            } else {

                while (timestamp.getTimestamp(ahead_server_id) != null &&
                        timestamp.getTimestamp(server_id) < Integer.decode(ahead_time_stamp)) {

                    synchronized (this) {

                        try {

                            this.wait(Constants.Discovery.LATENCYUPBOUND / 2);
                        } catch (InterruptedException e) {

                            logger.error(Constants.Messages.INTERRUPTED);
                        }
                    }
                }

                logger.debug("DataStore received update for " + ahead_server_id + " while waiting.");
            }
        }

        String query = incoming_request_line.getParameter(Constants.Tokens.QUERY);
        String version_number = incoming_request_line.getParameter(Constants.Tokens.VERSIONNUM);

        if (query == null || version_number == null) {

            logger.error("Bad request: query=" + query + ", version=" + version_number);
            setBadRequestResponse();
            return;
        }

        logger.debug("Searching for query: " + query);
        JSONObject search_results = new JSONObject();

        // Search the data store for the query
        String current_version_number = data_store.searchDataStore(query, version_number, search_results);

        // Compare the version numbers to determine if an update must be sent
        if (version_number.equals(current_version_number)) {

            logger.debug("Cache is current, no updates");
            response_body = new JSONObject();
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.NOTMODIFIED);

            response = SystemUtility.buildResponse(Constants.Codes.NOTMODIFIED, this.response_body);
        } else {

            if (!version_number.equals(current_version_number)) {

                logger.debug("Cache must update from " + version_number + " to " + current_version_number);
            } else {

                logger.debug("No search results for query: " + query);
            }

            // Prepare the response
            response_body = new JSONObject();
            response_body.put(Constants.Tokens.TWEETS, search_results);
            response_body.put(Constants.Tokens.QUERY, query);
            response_body.put(Constants.Tokens.VERSIONNUM, current_version_number);
            response_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());

            response = SystemUtility.buildResponse(Constants.Codes.OK, this.response_body);
        }
    }

    /**
     * Updates the data store with a new tweet and prepares a HTTP response.
     */
    private void updateDataStore() {

        String tweet = (String) request_body.get(Constants.Tokens.TWEET);

        List<String> hashtags = (List<String>) request_body.get(Constants.Tokens.HASH);

        // Validate the provided tweet and hashtag set
        if (tweet == null || hashtags == null || hashtags.isEmpty()) {

            logger.error("Bad request: tweet=" + tweet + ", hashtags=" + hashtags);
            setBadRequestResponse();
            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        timestamp.incrementTimestamp(this.server_id);
        data_store.postToDataStore(hashtags, tweet, timestamp);
        logger.debug("Tweet posted: " + tweet);

        // Prepare the response
        response_body = new JSONObject();
        response_body.put(Constants.Tokens.STATUS, Constants.Messages.CREATED);
        response_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());

        response = SystemUtility.buildResponse(Constants.Codes.CREATED, this.response_body);

        broadcastWriteToReplicas();
        timestamp.incrementTimestamp(this.server_id);
    }

    /**
     * Sends replication requests to all replicas.
     */
    private void broadcastWriteToReplicas() {

        request_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());
        request_body.put(Constants.Tokens.SERVERID, this.server_id);

        // Change URI path to /replicate and redirect tweet post request to all other data store servers
        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                Constants.Tokens.REPLICATE, null, request_body).toString();

        logger.debug("Broadcasting tweet to all other data store servers");
        for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

            String server_location = server_directory.getServerLocation(entry.getKey());
            if (!entry.getKey().equals(this.server_id)
                    && !entry.getKey().startsWith(Constants.Config.FRONTEND)
                    && server_location != null) {

                this.workers = new ArrayList<Thread>();
                ReplicationBroadcastWorker rbw = new ReplicationBroadcastWorker(server_location, request, testing_delay);
                this.workers.add(rbw);
                rbw.start();
            }
        }
    }

    /**
     * Updates the data store with a write extracted from a replication
     * request.
     */
    private void replicateIncomingData() {

        String tweet = (String) request_body.get(Constants.Tokens.TWEET);
        String server_id = (String) request_body.get(Constants.Tokens.SERVERID);
        JSONObject timestamp_json = (JSONObject) request_body.get(Constants.Tokens.TIMESTAMP);

        List<String> hashtags = (List<String>) request_body.get(Constants.Tokens.HASH);

        // Validate the provided tweet and hashtag set
        if ((tweet == null || hashtags == null || hashtags.isEmpty() ||server_id == null || timestamp_json == null)) {

            logger.error("Bad request: tweet=" + tweet + ", hashtags=" + hashtags + ", server_id=" + server_id +
                    "timestamp=" + timestamp_json);
            setBadRequestResponse();
            return;
        }

        timestamp.updateTimestamp(server_id, (String) timestamp_json.get(server_id));
        timestamp.incrementTimestamp(this.server_id);
        data_store.postToDataStore(hashtags, tweet, timestamp);
        logger.debug("Tweet replicated: " + tweet);
        logger.debug("New timestamp: " + timestamp.toString());

        response_body = new JSONObject();
        response_body.put(Constants.Tokens.STATUS, Constants.Messages.OK);

        response = SystemUtility.buildResponse(Constants.Codes.OK, this.response_body);
    }

    /**
     * Builds a copy of the data store for transmitting to the requesting
     * server.
     */
    private void replicateDataStore() {

        String server_id = (String) request_body.get(Constants.Tokens.SERVERID);
        JSONArray stamps_array = (JSONArray) request_body.get(Constants.Tokens.STAMPS);
        Integer stamp_min = (Integer) stamps_array.get(0);
        Integer stamp_max = (Integer) stamps_array.get(1);
        JSONObject timestamp_json = (JSONObject) request_body.get(Constants.Tokens.TIMESTAMP);
        timestamp.updateTimestamp(server_id, (String) timestamp_json.get(server_id));

        JSONObject data_store_copy = this.data_store.getDataStoreCopy(stamp_min, stamp_max, server_id);

        response_body = new JSONObject();
        if (data_store_copy.size() > 0) {

            response_code = Constants.Codes.OK;
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.OK);
            response_body.put(Constants.Tokens.REPLICATE, data_store_copy);
            response_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());
        } else {

            response_code = Constants.Messages.NOTMODIFIED;
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.NOTMODIFIED);
        }

        response = SystemUtility.buildResponse(response_code, this.response_body);
    }

    /**
     * Requests a data store update from another server.
     *
     * @param ahead_server_id Server ID of server for which an update is needed
     * @param ahead_time_stamp Timestamp value for which update is needed
     * @param known_time_stamp Timestamp value currently known
     * @param updater_id Server ID to which the update will be sent
     * @return Status code indicating success of update request
     */
    private String requestUpdate(String ahead_server_id, String ahead_time_stamp, String known_time_stamp, String updater_id) {

        String updater_loc = server_directory.getServerLocation(updater_id);
        HTTPObject replication_response = null;

        if (!ahead_server_id.startsWith(Constants.Config.FRONTEND) && updater_loc != null) {

            JSONArray stamp_bounds_array = new JSONArray();
            stamp_bounds_array.add(Integer.decode(known_time_stamp));
            stamp_bounds_array.add(Integer.decode(ahead_time_stamp));
            request_body.clear();
            request_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());
            request_body.put(Constants.Tokens.SERVERID, this.server_id);
            request_body.put(Constants.Tokens.STAMPS, stamp_bounds_array);

            String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET,
                    Constants.Tokens.REPLICATE, null, request_body).toString();

            String[] add_and_port = null;
            try {

                add_and_port = updater_loc.split(":");
                Socket replication_socket = new Socket(add_and_port[0], Integer.decode(add_and_port[1]));
                replication_response = SystemUtility.sendRequest(request, replication_socket);

                if (replication_response.getStatusCode().equals(Constants.Codes.OK)) {

                    JSONObject replication_json = replication_response.getBody();
                    JSONObject data_store_json = (JSONObject) replication_json.get(Constants.Tokens.REPLICATE);

                    timestamp.incrementTimestamp(this.server_id);
                    this.data_store.mergeReplicationData((JSONObject) data_store_json.get(Constants.Tokens.DATASTORE),
                            (JSONObject) data_store_json.get(Constants.Tokens.VERSIONMAP));

                }
            } catch (IOException e) {

                logger.error("Unable to open catch up request socket to: " + add_and_port[0] + ":" + add_and_port[1]);
            }
        }
        if (updater_loc == null) {

            return Constants.Codes.NOTMODIFIED;
        } else {

            return replication_response.getStatusCode();
        }
    }

    /**
     * Pick a server and query it for a data store update. This method is used
     * when a server comes online for the first time.
     */
    private void requestFullDataStoreUpdate() {

        for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

            String candidate_server_id = entry.getKey();
            String candidate_server_loc = entry.getValue();
            if (!candidate_server_id.equals(server_id) && candidate_server_loc != null) {

                // Break only if update is found
                if (requestUpdate(candidate_server_id, "-1", "-1", candidate_server_loc).equals(Constants.Codes.OK)) {

                    logger.debug("Data store update retrieved from " + candidate_server_id);
                    return;
                }
            }
        }
        logger.debug("Unable to find server for data store update request");
    }

    /**
     * Builds and sends a copy of the data store.
     */
    private void sendSnapshot() {

        if (testing_delay > 0) {

            synchronized (this) {

                try {

                    this.wait(testing_delay);
                } catch (InterruptedException e) {

                    logger.error(Constants.Messages.INTERRUPTED);
                }
            }
        }

        VectorTimestamp upper_bound_timestamp = null;

        // Check and wait for timestamp updates if a timestamp was required with the request
        if (request_body.containsKey(Constants.Tokens.TIMESTAMP)) {

            upper_bound_timestamp = new VectorTimestamp(new HashMap<String, String>((JSONObject) request_body.get(Constants.Tokens.TIMESTAMP)));

            while (timestamp.getTimestamp(this.server_id) < upper_bound_timestamp.getTimestamp(this.server_id)) {

                synchronized (this) {

                    try {

                        this.wait(Constants.Discovery.LATENCYUPBOUND / 2);
                        logger.debug("Waiting for replication before returning snapshot");
                    } catch (InterruptedException e) {

                        logger.error(Constants.Messages.INTERRUPTED);
                    }
                }
            }
        }

        response_body = new JSONObject();
        response_body.put(Constants.Tokens.SNAPSHOT, this.data_store.getDataStoreCopy(upper_bound_timestamp, server_id));
        response_body.put(Constants.Tokens.TIMESTAMP, this.timestamp.toJSONObject());

        response = SystemUtility.buildResponse(Constants.Codes.OK, response_body);
    }

    /**
     * Builds a reply to a heart beat monitoring request.
     */
    private void sendHeartBeatResponse() {

        response_body = new JSONObject();
        response_body.put(Constants.Tokens.STATUS, Constants.Messages.OK);

        response = SystemUtility.buildResponse(Constants.Codes.OK, this.response_body);
    }
}
