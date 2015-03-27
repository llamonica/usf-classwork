package test;

import main.*;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by luke on 11/5/14.
 */
public class ConsistencyBlockTest {

    public static void main(String[] args) {

        try {

            String[] location = SystemUtility.getServerConfig(Constants.Config.DATASTORE, "1", false);
            Socket socket = new Socket(location[0], Integer.decode(location[1]));

            HashMap<String, String> query_params = new HashMap<String, String>();
            query_params.put(Constants.Tokens.QUERY, "tweet");
            query_params.put(Constants.Tokens.VERSIONNUM, "1");

            HashMap<String, String> timestamp_map = new HashMap<String, String>();
            timestamp_map.put("datastore2", "6");
            VectorTimestamp timestamp = new VectorTimestamp(timestamp_map);

            JSONObject request_body = new JSONObject();
            request_body.put(Constants.Tokens.TIMESTAMP, timestamp);

            HTTPObject request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET,
                    Constants.Tokens.TWEETS, query_params, request_body);

            System.out.println("Delayed output:\n" + SystemUtility.sendRequest(request.toString(), socket));

        } catch (IOException e) {

            e.printStackTrace();
        }
    }
}
