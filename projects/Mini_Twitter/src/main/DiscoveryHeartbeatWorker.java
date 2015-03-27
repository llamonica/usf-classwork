package main;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Thread class for monitoring the health of a data store server.
 *
 * Created by luke on 10/29/14.
 */
public class DiscoveryHeartbeatWorker extends Thread {

    private ServerDirectory server_directory;
    private int timeout;
    private String server_id;
    private String server_location;
    private String server_ip;
    private int server_port;
    private boolean initial_wait_passed;

    final static Logger logger = Logger.getLogger(DiscoveryHeartbeatWorker.class);

    public DiscoveryHeartbeatWorker(ServerDirectory server_directory, int timeout, String server_id) {

        this.server_directory = server_directory;
        this.timeout = timeout;
        this.server_id = server_id;

        this.server_location = this.server_directory.getServerLocation(server_id);
        this.server_ip = server_location.split(":")[0];
        this.server_port = Integer.decode(server_location.split(":")[1]);

        this.initial_wait_passed = false;
    }

    /**
     * Uses a timer task to periodically send requests to the monitored server.
     * While the timer task is waiting for a response, the worker thread also
     * waits. The timer task notifies the worker thread when either a response
     * is received or its request times out. The worker thread then wakes and
     * checks the heart beat response for a response from the monitored server.
     * If a response exists, the worker thread waits for the next notification.
     * Otherwise, it registers a server failure and broadcasts the failure to
     * all other servers.
     */
    @Override
    public void run() {

        try {

            boolean alive = true;

            if (!initial_wait_passed) {

                synchronized (this) {

                    this.wait(timeout);
                }
                initial_wait_passed = true;
            }

            HeartBeatResponse heart_beat_response = new HeartBeatResponse();

            HTTPObject request = SystemUtility.buildRequest(HTTPConstants.HTTPMethod.GET,
                    Constants.Tokens.DISCOVER, null, null);

            TimerTask heart_beat_task = new HeartbeatTimerTask(request.toString(), server_ip, server_port, heart_beat_response);
            Timer timer = new Timer();

            logger.debug("Beginning heart beat request transmission");
            timer.scheduleAtFixedRate(heart_beat_task, 0, timeout);

            ArrayList<Thread> workers = null;

            while (alive) {

                synchronized (heart_beat_response) {

                    heart_beat_response.wait(timeout);
                }

                if (heart_beat_response.response == null) {
                    // Monitored server failed
                    logger.debug("Server failed, broadcasting death of server: " + server_id);

                    // Cancel the monitoring timer task
                    timer.cancel();

                    // Update the server directory master
                    server_directory.setServerFailed(server_id);

                    // Broadcast the updated server directory master
                    workers = new ArrayList<Thread>();
                    for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

                        if (server_directory.getServerLocation(entry.getKey()) != null) {

                            DiscoveryBroadcastWorker dbw = new DiscoveryBroadcastWorker(entry.getValue(), server_directory);
                            workers.add(dbw);
                            dbw.start();
                        }
                    }
                    alive = false;
                } else {
                    // Monitored server is still active

                    heart_beat_response.response = null;
                    logger.debug("Heart beat received from server: " + server_id);
                    synchronized (this) {

                        this.wait(this.timeout);
                    }
                }
            }

            // Join any running broadcast worker threads
            for (Thread worker : workers) {

                worker.join();
            }
        } catch (InterruptedException ignored) { }
    }

    /**
     * Timer task class for sending the heart beat requests to the monitored
     * server.
     */
    private class HeartbeatTimerTask extends TimerTask {

        private String request;
        private String server_ip;
        private int server_port;
        private HeartBeatResponse heart_beat_response;

        public HeartbeatTimerTask(String request, String server_ip, int server_port, HeartBeatResponse heart_beat_response) {

            this.request = request;
            this.server_ip = server_ip;
            this.server_port = server_port;
            this.heart_beat_response = heart_beat_response;
        }

        @Override
        public void run() {

            Socket heartbeat_socket = null;
            try {

                heartbeat_socket = new Socket(server_ip, server_port);
                heartbeat_socket.setSoTimeout(timeout / 2);
            } catch (SocketException e) {

                // Ignored
            } catch (UnknownHostException e) {

                // Ignored
            } catch (IOException e) {

                // Ignored
            }

            // Send request, notify waiting worker thread of response receipt
            heart_beat_response.response = SystemUtility.sendRequest(request, heartbeat_socket);
            synchronized (heart_beat_response) {

                heart_beat_response.notify();
            }
        }
    }

    /**
     * Wrapper class for passing the monitor request and for waiting and
     * notifying the waiting worker thread.
     */
    private class HeartBeatResponse {

        private HTTPObject response;

        public HeartBeatResponse() {

            this.response = null;
        }
    }
}
