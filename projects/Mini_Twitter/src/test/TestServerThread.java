package test;

import main.HTTPObject;
import main.HTTPServer;
import main.SystemUtility;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Thread class for running a server or a server socket in the background of
 * test classes.
 *
 * Created by luke on 9/29/14.
 */
public class TestServerThread extends Thread {

    private HTTPServer http_server;
    private ServerSocket server_socket;
    private Socket socket;
    private String input;
    private HTTPObject response;
    private HTTPObject request;

    final static Logger logger = Logger.getLogger(TestServerThread.class);

    public TestServerThread(HTTPServer http_server, ServerSocket server_socket, Socket request_socket) {

        this.http_server = http_server;
        this.server_socket = server_socket;
        this.socket = request_socket;
        this.input = "";
    }

    public void run() {

        // Activate server if it exists
        if (http_server != null) {

            http_server.serve();
        }

        // Send a request if a request socket was provided
        if (socket != null) {

            response = SystemUtility.sendRequest(request.toString(), socket);
        }

        // Activate server socket if it exists
        if (server_socket != null) {

            try {

                socket = server_socket.accept();
                logger.debug("Accepted connection on port: " + socket.getLocalPort());

                input = SystemUtility.readSocket(socket).toString();
                logger.debug("Read input: " + input);

            } catch (IOException ignored) {}

            logger.debug("Sending response:\n" + response.toString());
            SystemUtility.sendResponse(response.toString(), socket);
        }
    }

    /**
     * Returns the input received by server or socket.
     *
     * @return Input received through server or socket
     */
    public String getInput() {

        return input;
    }

    /**
     * Sets a response to send in order to mimic a server's behavior.
     *
     * @param response Response to send
     */
    public void setResponse(HTTPObject response) {

        this.response = response;
    }

    /**
     * Returns the number of heart beat requests received by the test server.
     *
     * @return Number of heart beat requests received
     */
    public int getHeartBeatCount() {

        return http_server.getHeartBeatCount();
    }

    /**
     * Sets a request to send in order to mimic a client's behavior.
     *
     * @param request Request to send
     */
    public void setRequest(HTTPObject request) {

        this.request = request;
    }

    /**
     * Returns the response the thread has stored.
     *
     * @return HTTPObject response
     */
    public HTTPObject getResponse() {

        return response;
    }
}
