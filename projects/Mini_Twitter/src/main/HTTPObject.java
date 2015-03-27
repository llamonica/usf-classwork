package main;

import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic class for supporting operations on both HTTP requests and responses.
 *
 * Created by luke on 10/7/14.
 */
public class HTTPObject {

    private String intro_line;
    private HTTPRequestLine request_line = null;
    private HashMap<String, String> header_map;
    private JSONObject body;

    public HTTPObject(String intro_line, HashMap<String, String> headers, JSONObject body) {

        this.intro_line = intro_line;
        this.header_map = headers;
        if (body == null) {

            body = new JSONObject();
        }
        this.body = body;
    }

    public HTTPObject(HTTPRequestLine request_line, HashMap<String, String> headers, JSONObject body) {

        this.request_line = request_line;
        this.intro_line = request_line.toString();
        this.header_map = headers;
        if (body == null) {

            body = new JSONObject();
        }
        this.body = body;
    }

    /**
     * Provides access to a particular HTTP header value mapped to the
     * provided key.
     *
     * @param header_tag Key for the requested HTTP header value
     * @return Value mapped to the provided header key
     */
    public String getHeader(String header_tag) {

        return header_map.get(header_tag);
    }

    /**
     * Provides access to the HTTP body.
     *
     * @return HTTPObject body
     */
    public JSONObject getBody() {

        return body;
    }

    /**
     * Provides access to the request line if this object represents an HTTP
     * request. The request line is parsed and converted to a HTTPRequestLine
     * object upon the first access request.
     *
     * @return HTTPRequestLine object representing the request line
     * @throws HTTPRequestException
     */
    public HTTPRequestLine getRequestLine() throws HTTPRequestException {

        if (request_line == null) {

            request_line = HTTPRequestLineParser.parse(intro_line);
        }

        return request_line;
    }

    /**
     * Provides access to the string representation of the status/request line.
     *
     * @return String representation of the status/request line
     */
    public String getStatusLine() {

        return intro_line;
    }

    /**
     * Provides access to the status code if this object represents a HTTP
     * response. NOTE: it is up to the caller to ensure this object represents
     * a response and not a request. The returned string is not guaranteed to
     * represent a response status code.
     *
     * @return String representation of the response status code
     */
    public String getStatusCode() {

        return intro_line.split(" ")[1];
    }

    /**
     * Constructs a string representation of the HTTP object for transmission.
     *
     * @return String representation of the HTTP object
     */
    public String toString() {

        String header_map_string = "";
        for (Map.Entry<String, String> entry : header_map.entrySet()) {

            header_map_string += entry.getKey() + ":" + entry.getValue() + "\n";
        }

        return intro_line + "\n" + header_map_string + "\n" + body;
    }
}
