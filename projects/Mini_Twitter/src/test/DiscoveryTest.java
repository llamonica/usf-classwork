package test;

import main.Constants;
import main.HTTPServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by luke on 11/4/14.
 */
public class DiscoveryTest {

    private static HTTPServer discovery_server;
    private static TestServerThread discovery_server_thread;
    private static HTTPServer data_store_server1;
    private static HTTPServer data_store_server2;
    private static HTTPServer data_store_server3;
    private static HTTPServer data_store_server4;
    private static HTTPServer data_store_server5;
    private static TestServerThread data_store_server_thread1;
    private static TestServerThread data_store_server_thread2;
    private static TestServerThread data_store_server_thread3;
    private static TestServerThread data_store_server_thread4;
    private static TestServerThread data_store_server_thread5;


    @Test
    public void registerMultipleNewDataStores() {

        discovery_server = new HTTPServer(Constants.Config.DISCOVERY, null, true, null);

        discovery_server_thread = new TestServerThread(discovery_server, null, null);
        discovery_server_thread.start();

        waitBetweenTests();
        waitBetweenTests();

        data_store_server1 = new HTTPServer(Constants.Config.DATASTORE, "1", true, null);

        data_store_server_thread1 = new TestServerThread(data_store_server1, null, null);
        data_store_server_thread1.start();

        waitBetweenTests();
        waitBetweenTests();

        data_store_server2 = new HTTPServer(Constants.Config.DATASTORE, "2", true, null);

        data_store_server_thread2 = new TestServerThread(data_store_server2, null, null);
        data_store_server_thread2.start();

        waitBetweenTests();
        waitBetweenTests();

        data_store_server3 = new HTTPServer(Constants.Config.DATASTORE, "3", true, null);

        data_store_server_thread3 = new TestServerThread(data_store_server3, null, null);
        data_store_server_thread3.start();

        waitBetweenTests();
        waitBetweenTests();

        data_store_server4 = new HTTPServer(Constants.Config.DATASTORE, "4", true, null);

        data_store_server_thread4 = new TestServerThread(data_store_server4, null, null);
        data_store_server_thread4.start();

        waitBetweenTests();
        waitBetweenTests();

        data_store_server5 = new HTTPServer(Constants.Config.DATASTORE, "5", true, null);

        data_store_server_thread5 = new TestServerThread(data_store_server5, null, null);
        data_store_server_thread5.start();

        synchronized (this) {

            try {

                this.wait(5 * Constants.Discovery.TIMEOUT);
            } catch (InterruptedException e) {

                e.printStackTrace();
            }
        }

        Assert.assertTrue(data_store_server_thread1.getHeartBeatCount() >= 4);
        Assert.assertTrue(data_store_server_thread2.getHeartBeatCount() >= 4);
        Assert.assertTrue(data_store_server_thread3.getHeartBeatCount() >= 4);
        Assert.assertTrue(data_store_server_thread4.getHeartBeatCount() >= 4);
        Assert.assertTrue(data_store_server_thread5.getHeartBeatCount() >= 4);
    }

    private static void waitBetweenTests() {

        synchronized (DiscoveryTest.class) {

            try {

                DiscoveryTest.class.wait(500);
            } catch (InterruptedException e) {

                e.printStackTrace();
            }
        }
    }
}
