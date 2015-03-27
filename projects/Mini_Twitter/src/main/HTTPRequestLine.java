package main;

import java.util.HashMap;
import java.util.Map;

/**
 * HTTPRequestLine is a data structure that stores a Java representation of the parsed Request-Line.
 **/
public class HTTPRequestLine {

	private HTTPConstants.HTTPMethod method;
	private String uripath;
	private HashMap<String, String> parameters;
	private String httpversion;
	
    public HTTPRequestLine() {
        method = null;
        uripath = null;
        parameters = new HashMap<String, String>();
        httpversion = null;
    }

    public HTTPRequestLine(HTTPConstants.HTTPMethod method, String uripath,
                           HashMap parameters, String httpversion) {

        this.method = method;
        this.uripath = uripath;
        this.parameters = parameters;
        this.httpversion = httpversion;
    }

    public void setMethod(HTTPConstants.HTTPMethod method) {
        this.method = method;
    }

    public HTTPConstants.HTTPMethod getMethod() {
        return this.method;
    }

    public void setUripath(String uripath) {
        this.uripath = uripath;
    }

    public String getUripath() {
        return this.uripath;
    }

    public void setParameter(String key, String value) {
        this.parameters.put(key, value);
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters.putAll(parameters);
    }

    public String getParameter(String key) {
        return this.parameters.get(key);
    }

    public void setHttpversion(String httpversion) {
        this.httpversion = httpversion;
    }

    public String getHttpversion() {
        return httpversion;
    }

    /**
     * Converts the HTTPRequestLine object into a standard HTTP request line
     * string.
     *
     * @return String version of the request line
     */
    public String toString() {

        String request_line_string = method.toString();
        request_line_string += " /" + uripath;
        if (parameters != null) {

            request_line_string += "?";

            for (Map.Entry entry : parameters.entrySet()) {

                request_line_string += entry.getKey() + "=" + entry.getValue() + "&";
            }

            if (request_line_string.endsWith("&")) {

                request_line_string = request_line_string.substring(0, request_line_string.length() - 1);
            }
        }

        request_line_string += " " + httpversion;

        return  request_line_string;
    }
}
