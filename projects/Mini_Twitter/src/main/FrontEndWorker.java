package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Worker thread for the front end server.
 *
 * Created by luke on 9/25/14.
 */
public class FrontEndWorker extends ServerWorkerThread {

    private String datastore_ip;
    private int datastore_port;
    private CacheStore cache;
    private JSONObject cache_update;
    private String target_id;
    private static VectorTimestamp timestamp = null;

    final static Logger logger = Logger.getLogger(FrontEndWorker.class);

    public FrontEndWorker(Socket in_socket, String server_id, boolean development) {

        super(in_socket, server_id, development);
        this.cache = CacheStore.getInstance();

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

        Socket request_socket;

        if (!validatePathAndMethod()) {

            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        try {

            if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.TWEETS)) {

                selectDataStoreServer();

                // Open a socket for sending a request to the data store server
                request_socket = new Socket(datastore_ip, datastore_port);

                if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                    logger.debug("Reading cache");
                    readCache(request_socket);
                    // Incorporate the response body into a valid HTTP response
                    response = SystemUtility.buildResponse(response_code, response_body);
                } else if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

                    logger.debug("Relaying tweet to data store");
                    relayTweet(request_socket);
                }
            } else if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.DISCOVER)) {

                if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.POST)) {

                    logger.debug("Updating server directory");
                    updateServerDirectory();
                    timestamp.fillMissingValues(server_directory);
                }
            } else if (incoming_request_line.getUripath().equals("/" + Constants.Tokens.SNAPSHOT)) {

                if (incoming_request_line.getMethod().equals(HTTPConstants.HTTPMethod.GET)) {

                    logger.debug("Retrieving server snapshots");
                    relaySnapshotRequest();
                }
            }
        } catch (IOException e) {

            logger.error("Server socket failed");

            // Prepare an internal server error response if the socket fails
            response_code = Constants.Codes.INTERNALSERVERERROR;
            response_body = new JSONObject();
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.INTERNALSERVERERROR);

            response = SystemUtility.buildResponse(response_code, this.response_body);
        }
    }

    /**
     * Reads the cache for a given query if the version number is valid.
     *
     * @param request_socket Socket for sending the cache validation request
     */
    private void readCache(Socket request_socket) {

        String query = incoming_request_line.getParameter(Constants.Tokens.QUERY);

        // Validate the query
        // (Note: queries containing a space are filtered in the HTTPRequestLine parsing)
        if (query == null) {

            logger.error("Bad request: missing query");
            setBadRequestResponse();
            return;
        }

        // Retrieve the cache status for the query
        HTTPObject cache_validation_response = validateCache(query, request_socket);
        String cache_validation_code = cache_validation_response.getStatusCode();

        // Update the cache if 200 was received as response code
        if (cache_validation_code.equals(Constants.Codes.OK)) {

            cache_update = cache_validation_response.getBody();

            cache.writeCache((String) cache_update.get(Constants.Tokens.QUERY),
                    (JSONObject) cache_update.get(Constants.Tokens.TWEETS),
                    (String) cache_update.get(Constants.Tokens.VERSIONNUM));

            logger.debug("Updated cache for query=" + query + " to version=" +
                    cache_update.get(Constants.Tokens.VERSIONNUM));
        }

        // Verify the validation request was processed successfully
        if (!cache_validation_code.equals(Constants.Codes.NOTMODIFIED) &&
                !cache_validation_code.equals(Constants.Codes.OK)) {

            logger.error("Cache validation failed: " + cache_validation_code);
            response_code = cache_validation_code;
            response = cache_validation_response;
            return;
        }

        // Prepare the search results for the response
        if (cache_update != null) {

            logger.debug("Cache updated");
        } else {

            logger.debug("Cache is current - reading cache");
        }

        // Prepare the response
        response_code = Constants.Codes.OK;
        response_body = new JSONObject();
        response_body.put(Constants.Tokens.TWEETS, cache.readCache(query));
        response_body.put(Constants.Tokens.QUERY, query);
        cache_update = null;
    }

    /**
     * Sends a validation request to the data store for the query and its
     * version number in the cache.
     *
     * @param query Query to be validated
     * @param request_socket Socket for sending the cache validation request
     * @return Response to the validation request
     */
    private HTTPObject validateCache(String query, Socket request_socket) {

        HashMap<String, String> query_params = new HashMap<String, String>();
        query_params.put(Constants.Tokens.QUERY, query);
        query_params.put(Constants.Tokens.VERSIONNUM, cache.getVersionNumber(query));

        request_body.put(Constants.Tokens.TIMESTAMP, timestamp.toJSONObject());
        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET,
                Constants.Tokens.TWEETS, query_params, request_body).toString();

        logger.debug("Sending request to validate cache");

        return SystemUtility.sendRequest(request, request_socket);
    }

    /**
     * Parses the tweet and the tweet's hashtags from the client request. If
     * the tweet and at least one hashtag are found, the request is relayed to
     * the data store server.
     *
     * @param request_socket Socket for relaying the client request
     */
    private void relayTweet(Socket request_socket) {

        String tweet = (String) request_body.get(Constants.Tokens.TWEET);

        // Validate the tweet
        if (tweet == null) {

            logger.error("Tweet missing");
            setBadRequestResponse();
            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        // Parse hashtags from the request's tweet
        Matcher hashtag_matcher = Pattern.compile("#(\\S)+").matcher(tweet);
        JSONArray hashtags = new JSONArray();

        while (hashtag_matcher.find()) {

            hashtags.add(hashtag_matcher.group().substring(1));
        }

        // Verify at least one hashtag was included
        if (hashtags.size() == 0) {

            logger.error("Tweet with no hashtags rejected");
            setBadRequestResponse();
            response = SystemUtility.buildResponse(response_code, response_body);
            return;
        }

        // Modify the request body before sending it to the data store server
        request_body.put(Constants.Tokens.HASH, hashtags);

        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                Constants.Tokens.TWEETS, null, request_body).toString();

        logger.debug("Sending tweet to data store");
        response = SystemUtility.sendRequest(request, request_socket);
        response_code = response.getStatusCode();

        JSONObject response_json_obj;
        JSONObject timestamp_json_obj;

        response_json_obj = response.getBody();
        timestamp_json_obj = (JSONObject) response_json_obj.get(Constants.Tokens.TIMESTAMP);

        timestamp.updateTimestamp(new HashMap<String, String>(timestamp_json_obj));
    }

    /**
     * Chooses a destination data store server to send a request. Front end
     * servers will choose the same server each time based on their server ID
     * instance number. If the data store server is down, a new server is
     * selected until the old one is brought online.
     */
    private void selectDataStoreServer() {

        String[] location;
        if (development && request_body.containsKey(Constants.Tokens.SERVERID)) {

            target_id = (String) request_body.get(Constants.Tokens.SERVERID);
        } else {

            String instance = this.server_id.replace(Constants.Config.FRONTEND, "");
            target_id = Constants.Config.DATASTORE + instance;

            if (server_directory.getServerLocation(target_id) == null) {

                ArrayList<String> candidate_ids = new ArrayList<String>();
                for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

                    if (!entry.getKey().startsWith(Constants.Config.FRONTEND) && entry.getValue() != null) {

                        candidate_ids.add(entry.getKey().replace(Constants.Config.DATASTORE, ""));
                    }
                }

                Collections.sort(candidate_ids, new Comparator<String>() {
                    @Override
                    public int compare(String s, String s2) {
                        return Integer.decode(s) - Integer.decode(s2);
                    }
                });

                for (String candidate_id : candidate_ids) {

                    int candidate_idx = Integer.decode(candidate_id.replace(Constants.Config.DATASTORE, ""));
                    if (Integer.decode(instance) < candidate_idx) {

                        target_id = candidate_id;
                        break;
                    }
                }
            }
        }
        location = server_directory.getServerLocation(target_id).split(":");

        this.datastore_ip = location[0];
        this.datastore_port = Integer.decode(location[1]);
    }

    /**
     * Relays a snapshot request to each of the data store servers. If
     * incongruous snapshots are received in return, a second request is sent
     * for each outdated server snapshot.
     */
    public void relaySnapshotRequest() {

        HashMap<String, JSONObject> collection_map = new HashMap<String, JSONObject>();
        HashMap<String, VectorTimestamp> timestamp_map = new HashMap<String, VectorTimestamp>();
        workers = new ArrayList<Thread>();

        // Request snapshots from each data store server
        sendSnapshotRequestsViaWorkers(null, null);

        // Gather the snapshots into snapshot collection and timestamp maps
        collectDataFromWorkers(collection_map, timestamp_map);

        // Determine which if any servers returned an incongruous snapshot
        HashSet<String> outdated_servers = new HashSet<String>();

        HashMap<String, String> upper_bound_timestamp_map = new HashMap<String, String>();

        for (Map.Entry<String, VectorTimestamp> server_entry : timestamp_map.entrySet()) {

                for (Map.Entry<String, String> timestamp_entry : server_entry.getValue().getTimestampMap().entrySet()) {

                    String candidate_server_id = timestamp_entry.getKey();
                    Integer stamp_val_from_candidate = timestamp_map.get(candidate_server_id).getTimestamp(candidate_server_id);

                    if (!upper_bound_timestamp_map.containsKey(candidate_server_id) ||
                            Integer.decode(upper_bound_timestamp_map.get(candidate_server_id)) < stamp_val_from_candidate) {

                        upper_bound_timestamp_map.put(candidate_server_id, stamp_val_from_candidate.toString());
                    }

                if (!server_entry.getKey().equals(candidate_server_id) &&
                        Integer.decode(timestamp_entry.getValue()) > stamp_val_from_candidate) {

                    if (!outdated_servers.contains(candidate_server_id)) {

                        outdated_servers.add(candidate_server_id);

                        logger.debug("Found outdated server " + candidate_server_id);
                    }
                }
            }
        }

        // Send second round requests to any servers that provided incongruous snapshots
        if (outdated_servers.size() > 0) {

            VectorTimestamp upper_bound_timestamp = new VectorTimestamp(upper_bound_timestamp_map);
            sendSnapshotRequestsViaWorkers(outdated_servers, upper_bound_timestamp);
            collectDataFromWorkers(collection_map, timestamp_map);
        }

        response_code = Constants.Codes.OK;
        response_body = new JSONObject();
        response_body.put(Constants.Tokens.SNAPSHOT, new JSONObject(collection_map));

        response = SystemUtility.buildResponse(Constants.Codes.OK, response_body);
    }

    /**
     * Starts a set of worker threads to send snapshot requests to each data
     * store server.
     *
     * @param outdated_servers Optional list of target servers
     * @param upper_bound_timestamp Optional upper bound timestamp to be sent
     */
    public void sendSnapshotRequestsViaWorkers(HashSet<String> outdated_servers, VectorTimestamp upper_bound_timestamp) {

        for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

            if (!entry.getKey().startsWith(Constants.Config.FRONTEND) && entry.getValue() != null &&
                    (outdated_servers == null || outdated_servers.contains(entry.getKey()))) {

                JSONObject snapshot_request_body = null;
                if (outdated_servers != null) {

                    snapshot_request_body = new JSONObject();
                    snapshot_request_body.put(Constants.Tokens.TIMESTAMP, upper_bound_timestamp.toJSONObject());
                    logger.debug("Sending snapshot update request to " + entry.getKey());
                } else {

                    logger.debug("Sending first round snapshot request to " + entry.getKey());
                }

                HTTPObject snapshot_request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET,
                        Constants.Tokens.SNAPSHOT, null, snapshot_request_body);

                SnapshotRequestWorker srw = new SnapshotRequestWorker(entry.getValue(), snapshot_request.toString(), entry.getKey());
                srw.start();
                workers.add(srw);
            }
        }
    }

    /**
     * Collects the snapshot responses from the snapshot request workers.
     *
     * @param collection_map Map to which data store collections are written
     * @param timestamp_map Map to which timestamps are written
     */
    public void collectDataFromWorkers(HashMap<String, JSONObject> collection_map, HashMap<String, VectorTimestamp> timestamp_map) {

        for (Thread worker : workers) {

            try {

                worker.join();
                HTTPObject snapshot_response = ((SnapshotRequestWorker) worker).getResponse();

                if (snapshot_response != null) {

                    logger.debug("Received snapshot response from " + ((SnapshotRequestWorker) worker).getServerID());

                    JSONObject snapshot_response_body = snapshot_response.getBody();
                    String snapshot_server_id = ((SnapshotRequestWorker) worker).getServerID();

                    collection_map.put(snapshot_server_id,
                            (JSONObject) snapshot_response_body.get(Constants.Tokens.SNAPSHOT));

                    timestamp_map.put(snapshot_server_id,
                            new VectorTimestamp(
                                    new HashMap((JSONObject) snapshot_response_body.get(Constants.Tokens.TIMESTAMP))));
                }
            } catch (InterruptedException ignored) { }
        }

        workers.clear();
    }
}
