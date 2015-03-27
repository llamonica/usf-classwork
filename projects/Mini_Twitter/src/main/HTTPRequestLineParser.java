package main;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;


public class HTTPRequestLineParser {

	/**
	 * This method takes as input the Request-Line exactly as it is read from the socket.
	 * It returns a Java object of type main.HTTPRequestLine containing a Java representation of
	 * the line.
	 *
	 * The signature of this method may be modified to throw exceptions you feel are appropriate.
	 * The parameters and return type may not be modified.
	 *
	 * 
	 * @param line
	 * @return
	 */
	public static HTTPRequestLine parse(String line) throws HTTPRequestException {
	    //A Request-Line is a METHOD followed by SPACE followed by URI followed by SPACE followed by VERSION
	    //A VERSION is 'HTTP/' followed by 1.0 or 1.1
	    //A URI is a '/' followed by PATH followed by optional '?' PARAMS 
	    //PARAMS are of the form key'='value'&'

        HTTPRequestLine httpRequestLine = new HTTPRequestLine();

        //List the components of the request line
        ArrayList<String> req_line_list = new ArrayList<String>();
        Collections.addAll(req_line_list, line.split(" "));

        //Verify there are exactly three components
        if (req_line_list.size() != 3) {

            throw new HTTPRequestException("Invalid number of request line components in: " + line);
        }

        URI uri;
        try {

            //Retrieve method, throwing IllegalArgumentException if necessary
            HTTPConstants.HTTPMethod method = HTTPConstants.HTTPMethod.valueOf(req_line_list.remove(0));
            httpRequestLine.setMethod(method);

            //Use URI built-in to rely on its parsing rules for URI syntax
            uri = new URI(req_line_list.remove(0));
        } catch (IllegalArgumentException iae) {

            throw new HTTPRequestException("Unsupported HTTP method");
        } catch (URISyntaxException ue) {

            throw new HTTPRequestException("Invalid URI syntax");
        }

        //Retrieve and validate the path
        String path = uri.getPath();
        int path_start = path.indexOf("/");
        if (path_start > -1) {

            path = path.substring(path_start);
            httpRequestLine.setUripath(path);
        } else {

            throw new HTTPRequestException("Unsupported URI");
        }

        //Verify there is no fragment
        if (uri.getFragment() != null) {

            throw new HTTPRequestException("Unsupported fragment");
        }

        //Build a list of query components, splitting on either ? or &
        ArrayList<String> query_list = new ArrayList<String>();
        Pattern query_pattern = Pattern.compile("[&\\?]");
        //Use URI built-in to rely on its query syntax rules
        String query_temp = uri.getQuery();
        query_temp = (query_temp != null ? query_temp : "");
        Collections.addAll(query_list, query_pattern.split(query_temp));

        //Map the query key-value pairs, discarding any key duplicates
        for (String query : query_list) {
            String key = query.split("=")[0];
            String value = query.split("=").length == 2 ? query.split("=")[1] : "";
            httpRequestLine.setParameter(key, value);
        }

        //Retrieve the HTTP version
        String httpversion = req_line_list.get(0);
        String[] version_components = httpversion.split("/");

        //Validate the format and version numbers of the HTTP version
        if (version_components.length == 2 &&
                version_components[0].equals("HTTP") &&
                (version_components[1].equals("1.0") ||
                version_components[1].equals("1.1"))) {

            httpRequestLine.setHttpversion(httpversion);
        } else {

            throw new HTTPRequestException("Unsupported HTTP version");
        }

        return httpRequestLine;
	}

    public static void main(String[] args) {

        List<String> accept_test_cases = Arrays.asList(
                "GET /restaurantmashup HTTP/1.0",
                "GET /restaurant HTTP/1.0",
                "GET http://bla.bla.com/restaurant HTTP/1.0",
                "GET /restaurantmashup?city=las_vegas HTTP/1.0",
                "DELETE /restaurantmashup?city=las_vegas&city=san_francisco HTTP/1.0",
                "POST /restaurantmashup HTTP/1.0",
                "OPTIONS /restaurantmashup HTTP/1.0",
                "GET /restaurantmashup HTTP/1.1",
                "GET /restaurantmashup? HTTP/1.1",
                "GET /restaurantmashup&q= HTTP/1.1",
                "GET /restaurantmashup?q= HTTP/1.1",
                "GET /restaurantmashup?q HTTP/1.1",
                "GET /restaurantmashup?city=las_vegas?city=san_francisco HTTP/1.0"
        );

        List<String> fail_test_cases = Arrays.asList(
                "get /restaurantmashup HTTP/1.0",
                "GET /restaurantmashup HtTP/1.0",
                "GET /restaurantmashup HTTP/1.00",
                "GET /restaurantmashup HTTP /1.0",
                "GETT /restaurantmashup HTTP/1.0",
                "GET restaurantmashup HTTP/1.0",
                "GET /restaurantmashup HTTP/ 1.0",
                "GET /restaurantmashup#bla?q= HTTP/1.1",
                "GET /restaurantmashup?q=bla HTTP/1.0 asdfjkl;",
                "GET /restaurantmashup?city=las vegas HTTP/1.0"
        );

        int passed = 0;
        int total = accept_test_cases.size() + fail_test_cases.size();

        System.out.println("The following tests should parse successfully.\n");

        for (String test_case : accept_test_cases) {

            System.out.println("Testing: " + test_case);
            try {

                HTTPRequestLineParser.parse(test_case);
                System.out.println("Passed: Request was parsed");
                passed++;
            } catch (HTTPRequestException e) {

                System.out.println("Failed: " + e.getMessage());
            }
        }

        System.out.println("\n\nThe following tests should throw exceptions.\n");

        for (String test_case : fail_test_cases) {

            System.out.println("Testing: " + test_case);
            try {

                HTTPRequestLineParser.parse(test_case);
                System.out.println("Failed: Exception expected");
            } catch (HTTPRequestException e) {

                System.out.println("Passed: " + e.getMessage());
                passed++;            }
        }

        System.out.println("\n\n" + passed + " passed out of " + total);
    }
}
