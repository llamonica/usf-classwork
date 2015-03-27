package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Base worker thread class for the two system layers.
 *
 * Created by luke on 9/25/14.
 */
public abstract class ServerWorkerThread implements Runnable {

    protected HTTPRequestLine incoming_request_line;
    protected JSONObject request_body;
    protected Socket input_socket;
    protected String response_code;
    protected JSONObject response_body;
    protected HTTPObject response;
    protected String server_id;
    protected boolean development;
    protected static ServerDirectory server_directory = null;
    protected HashSet<String> valid_paths = null;
    protected ArrayList<Thread> workers = null;

    final static Logger logger = Logger.getLogger(ServerWorkerThread.class);

    public ServerWorkerThread(Socket input_socket, String server_id, boolean development) {

        this.input_socket = input_socket;
        this.server_id = server_id;
        this.development = development;
    }

    /**
     * Validates the URI path and method of a request by comparing to
     * supported values. Assigns an appropriate response code and response body
     * in the event of an invalid path or invalid method.
     *
     * @return A boolean flag indicating the validity of the request line
     */
    protected boolean validatePathAndMethod() {

        if (!this.valid_paths.contains(incoming_request_line.getUripath())) {

            logger.error(Constants.Messages.BADPATH + ": " + incoming_request_line.getUripath());
            response_code = Constants.Codes.NOTFOUND;
            response_body = new JSONObject();
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.BADPATH + ": " + incoming_request_line.getUripath());

            return false;
        }

        if (incoming_request_line.getMethod() != HTTPConstants.HTTPMethod.GET &&
                incoming_request_line.getMethod() != HTTPConstants.HTTPMethod.POST) {

            logger.error(Constants.Messages.BADMETHOD + ": " + incoming_request_line.getMethod());
            response_code = Constants.Codes.NOTFOUND;
            response_body = new JSONObject();
            response_body.put(Constants.Tokens.STATUS, Constants.Messages.BADMETHOD + ": " + incoming_request_line.getMethod());

            return false;
        }

        response_code = Constants.Codes.OK;
        return true;
    }

    /**
     * Sets the response code and response body to standard "bad request"
     * content.
     */
    protected void setBadRequestResponse() {

        response_code = Constants.Codes.BADREQUEST;
        response_body = new JSONObject();
        response_body.put(Constants.Tokens.STATUS, Constants.Messages.BADREQUEST);
    }

    /**
     * Abstract class to be specified by the server worker thread classes.
     */
    protected abstract void processRequest();

    /**
     * Extracts the required data from the provided HTTP object, including the
     * request line and body.
     *
     * @param http_object HTTP object containing required request line and body
     * @throws HTTPRequestException
     * @throws ParseException
     */
    protected void parseRequest(HTTPObject http_object) throws HTTPRequestException {

        // Extract the request line
        incoming_request_line = http_object.getRequestLine();

        // Extract the request body
        request_body = http_object.getBody();
    }

    /**
     * Generic run method for all server thread sub-classes. First parses the
     * request into useful components and then calls the sub-class
     * implementation for processing the request. The sub-classes set the
     * response code and response body while processing the request. The run
     * method finishes by sending the prepared response back to the client and
     * waiting for any running replication threads.
     */
    @Override
    public void run() {

        try {

            parseRequest(SystemUtility.readSocket(input_socket));
            processRequest();
        } catch (HTTPRequestException e) {

            // Unable to parse the request line
            logger.error("Unable to parse request: " + e.getMessage());
            setBadRequestResponse();
            response = SystemUtility.buildResponse(response_code, response_body);
        }

        //Send response
        if (response != null && !input_socket.isInputShutdown() && !input_socket.isOutputShutdown()) {

            SystemUtility.sendResponse(response.toString(), input_socket);
        } else {

            try {

                input_socket.close();
            } catch (IOException ignored) { }
        }

        //Wait for any replicator threads
        if (workers != null) {

            for (Thread worker : workers) {

                try {

                    worker.join();
                } catch (InterruptedException ignored) { }
            }
        }
    }

    /**
     * Retrieves a server directory update from the request object and
     * updates the local server directory with the retrieved directory.
     */
    protected void updateServerDirectory() {

        JSONObject replacement_server_map = (JSONObject) request_body.get(Constants.Tokens.SERVERS);

        if (replacement_server_map == null) {

            return;
        }

        if (server_directory == null) {

            server_directory = ServerDirectory.getInstance(new HashMap<String, String>(replacement_server_map),
                    this.development);
        } else {

            server_directory.updateDirectoryMapping(replacement_server_map);
        }
    }
}
