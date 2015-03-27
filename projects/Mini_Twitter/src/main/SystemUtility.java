package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

/**
 * A utility class for providing various functionality to all classes. This
 * class provides means for sending and receiving requests, sending responses,
 * preparing requests and responses, extracting elements from a request or
 * response, retrieving server configurations, and preparing HTML responses.
 *
 * Created by luke on 9/27/14.
 */
public class SystemUtility {

    final static Logger logger = Logger.getLogger(SystemUtility.class);
    private static JSONObject server_loc_map = null;

    /**
     * Builds a standard HTTP response for the given parameters.
     *
     * @param response_code Response code
     * @param response_body Response body
     * @return HTTPObject representing a standard HTTP response
     */
    public static HTTPObject buildResponse(String response_code, JSONObject response_body) {

        HashMap<String, String> header_map = new HashMap<String, String>();
        header_map.put(Constants.Tokens.CONTENTLENGTH,
                String.valueOf(response_body.toJSONString().getBytes().length));

        return new HTTPObject(Constants.Tokens.HTTPVERSION + " " + response_code,
                                header_map, response_body);
    }

    /**
     * Builds a standard HTTP request for the given parameters.
     *
     * @param method HTTP method
     * @param path URI path
     * @param parameters URI parameters
     * @param request_body Request body
     * @return HTTPObject representing a standard HTTP request
     */
    public static HTTPObject buildRequest(HTTPConstants.HTTPMethod method, String path,
                                      HashMap parameters, JSONObject request_body) {

        if (request_body == null) {

            request_body = new JSONObject();
        }

        HTTPRequestLine http_request_line = new HTTPRequestLine(method,
                path, parameters, Constants.Tokens.HTTPVERSION);

        HashMap<String, String> header_map = new HashMap<String, String>();
        header_map.put(Constants.Tokens.CONTENTLENGTH, String.valueOf(request_body.toJSONString().length()));

        return new HTTPObject(http_request_line, header_map, request_body);
    }

    /**
     * Returns HTML with the exception message embedded in the HTML title and
     * body. Minimal HTML code is from:
     * http://www.sitepoint.com/a-minimal-html-document/
     *
     * @param response_body Response body
     * @param response_code Response code
     * @return HTML document with exception message embedded
     */
    public static String makeHTMLResponse(String response_body, String response_code) {

        return "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\n" +
                "    \"http://www.w3.org/TR/html4/strict.dtd\">\n" +
                "<html lang=\"en\">\n" +
                "  <head>\n" +
                "    <meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\">\n" +
                "    <title>" + response_code + "</title>\n" +
                "  </head>\n" +
                "  <body>\n" +
                response_body +
                "  </body>\n" +
                "</html>";
    }

    /**
     * Extracts the response code from a standard HTTP response.
     *
     * @param response Response from which the code is extracted
     * @return Response code
     */
    public static String stripResponseCode(String response) {

        return response.split("\n")[0].split(" ")[1];
    }

    /**
     * Extracts the response body from a standard HTTP response.
     *
     * @param response Response from which the body is extracted
     * @return Response body
     */
    public static String stripResponseBody(String response) {

        String[] components = response.split("\n\n");
        if (components.length == 2) {

            return components[1];
        } else {

            return "";
        }
    }

    /**
     * Retrieves the server configuration for the system layer and instance.
     *
     * @param target_layer System layer
     * @param instance Layer component instance
     * @param development Environment flag for choosing ports
     * @return An array with the server's IP and port
     */
    public static String[] getServerConfig(String target_layer, String instance, boolean development) {

        if (server_loc_map == null) {

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
            if (development) {

                json_map = (JSONObject) json_map.get(Constants.Config.DEV);
            } else {

                json_map = (JSONObject) json_map.get(Constants.Config.PRODUCTION);
            }
            server_loc_map = ((JSONObject) json_map.get(Constants.Config.LOCS));
        }

        JSONArray location;
        String ip;
        String port;

        // Retrieve the appropriate IP and port
        if (target_layer.equals(Constants.Config.FRONTEND)) {

            location = (JSONArray) server_loc_map.get(Constants.Config.FRONTEND + instance);
            ip = (String) location.get(0);
            port = (String) location.get(1);
        } else if (target_layer.equals(Constants.Config.DATASTORE)) {

            location = (JSONArray) server_loc_map.get(Constants.Config.DATASTORE + instance);
            ip = (String) location.get(0);
            port = (String) location.get(1);
        } else if (target_layer.equals(Constants.Config.DISCOVERY)) {

            location = (JSONArray) server_loc_map.get(Constants.Config.DISCOVERY);
            ip = (String) location.get(0);
            port = (String) location.get(1);
        } else {

            ip = null;
            port = null;
        }

        return new String[] {ip, port};
    }

    /**
     * Sends a server registration request to the discovery server.
     *
     * @param server_id ID of server to be registered
     * @param server_location Location of server to be registered
     * @param layer System layer of the server to be registered
     * @param discovery_server_location Location of discovery server
     */
    public static void registerWithDiscoveryServer(String server_id, String[] server_location, String layer, String[] discovery_server_location) {

        JSONObject request_body = new JSONObject();
        request_body.put(Constants.Tokens.SERVERID, server_id);
        request_body.put(Constants.Tokens.SERVERLOC, server_location[0] + ":" + server_location[1]);
        request_body.put(Constants.Tokens.LAYER, layer);
        String request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.POST,
                Constants.Tokens.DISCOVER, null, request_body).toString();

        try {

            sendRequestWithoutWait(request,
                    new Socket(discovery_server_location[0], Integer.decode(discovery_server_location[1])));
        } catch (IOException e) {

            logger.error("Unable to initialize server socket at: " +
                    discovery_server_location[0] + ":" + discovery_server_location[1]);
            System.exit(1);
        }
    }

    /**
     * Convenience method for getting a location string from a socket.
     *
     * @param socket Socket to be located
     * @return String representation of socket location
     */
    public static String getSocketLocation(Socket socket) {

        return socket.getInetAddress() + ":" + socket.getLocalPort();
    }

    /**
     * Reads input from the given socket.
     *
     * @param socket Socket from which to read input
     * @return HTTPObject representing the input string read from the socket
     */
    public static HTTPObject readSocket(Socket socket) {

        BufferedReader in;
        HTTPObject http_object = null;

        try {

            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String input = "";
            String input_seg;

            // Read socket input stream for request line and header
            input_seg = in.readLine();
            while (input_seg != null && !input_seg.equals("")) {

                input += input_seg + "\n";
                input_seg = in.readLine();
            }

            String[] requestComponents = input.split("\n");

            // Read the request body if there is one
            if (requestComponents.length > 1) {

                // Parse the request header for the content-length
                HashMap<String, String> header_map = new HashMap<String, String>();
                for (int i = 1; i < requestComponents.length; i++) {

                    String[] header_components = requestComponents[i].split(":");
                    header_map.put(header_components[0].trim().toLowerCase(), header_components[1].trim());
                }

                char[] request_body_array = null;
                int content_length = -1;
                String request_body;
                if (header_map.get(Constants.Tokens.CONTENTLENGTH) != null) {

                    content_length = Integer.decode(header_map.get(Constants.Tokens.CONTENTLENGTH));
                    request_body_array = new char[content_length];
                }

                // Read socket input stream for request body
                if (content_length != -1 && content_length == in.read(request_body_array, 0, content_length)) {

                    request_body = new String (request_body_array);

                    JSONObject request_body_json = null;
                    try {

                        request_body_json = (JSONObject) new JSONParser().parse(request_body);
                    } catch (ParseException e) {

                        logger.error("Unable to parse HTTP body: " + request_body);
                    }
                    http_object = new HTTPObject(requestComponents[0], header_map, request_body_json);
                    input = input + "\n" + request_body;
                } else {

                    // Content length is zero or not supplied, so add empty string as body
                    http_object = new HTTPObject(requestComponents[0], header_map, null);
                }
            } else {

                // No headers provided
                http_object = new HTTPObject(requestComponents[0], null, null);
            }

            logger.debug("Server " + getSocketLocation(socket) + " received input:\n" + input);
        } catch (IOException se) {

            logger.error("Unable to write to socket output stream");
        }

        return http_object;
    }

    /**
     * Sends a request on a given socket and waits for the response.
     *
     * @param request HTTP request to send
     * @param socket Socket to which the request is written
     * @return HTTPObject representing the response to the given request
     */
    public static HTTPObject sendRequest(String request, Socket socket) {

        OutputStream out = null;
        HTTPObject response = null;

        try {

            // Write the request to the socket
            out = socket.getOutputStream();
            out.write(request.getBytes());
            out.flush();

            // Read the socket for the response
            response = readSocket(socket);
        } catch (IOException se) {

            logger.error("Unable to write request:\n" + request);
        } finally {

            try {

                if (out != null && socket != null) {

                    out.close();
                    socket.close();
                }
            } catch (IOException ignored) {}
        }

        return response;
    }

    /**
     * Sends a request on a given socket and returns without waiting for the response.
     *
     * @param request HTTP request to send
     * @param socket Socket to which the request is written
     */
    public static void sendRequestWithoutWait(String request, Socket socket) {

        OutputStream out = null;

        try {

            // Write the request to the socket
            out = socket.getOutputStream();
            out.write(request.getBytes());
            out.flush();

        } catch (IOException se) {

            logger.error("Unable to write request:\n" + request);
        } finally {

            try {

                out.close();
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Sends a HTTP response on a given socket.
     *
     * @param response HTTP response to send
     * @param socket Socket to which the response is written
     */
    public static void sendResponse(String response, Socket socket) {

        OutputStream out = null;

        try {

            // Write the response to the socket
            out = socket.getOutputStream();
            out.write(response.getBytes());
            out.flush();

        } catch (IOException se) {

            logger.error("Unable to write response:\n" + response);
        } finally {

            try {

                out.close();
                socket.close();
            } catch (IOException ignored) {}
        }
    }
}
