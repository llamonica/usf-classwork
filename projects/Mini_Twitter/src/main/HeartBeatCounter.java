package main;

/**
 * Convenience class for tracking the number of heart beat monitor requests
 * a server has received. This class is meant to be used for testing the
 * discovery server.
 *
 * Created by luke on 11/4/14.
 */
public class HeartBeatCounter {

    private int count;

    public HeartBeatCounter() {

        count = 0;
    }

    public int getCount() {

        return count;
    }

    public void incrementCount() {

        count++;
    }
}
