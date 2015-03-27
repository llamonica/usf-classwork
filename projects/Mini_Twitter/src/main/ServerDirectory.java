package main;

import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * A server mapping class that stores a map of server IDs to their locations.
 *
 * Created by luke on 10/29/14.
 */
public class ServerDirectory {

    private static ServerDirectory singleton_server_directory = null;
    private HashMap<String, String> location_map;
    private MultiReaderLock update_lock;


    public synchronized static ServerDirectory getInstance(HashMap<String, String> mapping, boolean dev) {

        if (!dev && singleton_server_directory == null) {

            singleton_server_directory = new ServerDirectory(mapping);
        } else if (dev) {

            return new ServerDirectory(mapping);
        }
        return singleton_server_directory;
    }

    private ServerDirectory(HashMap<String, String> mappings) {

        if (mappings != null) {

            this.location_map = new HashMap<String, String>(mappings);
        } else {

            this.location_map = new HashMap<String, String>();
        }
        this.update_lock = new MultiReaderLock(-1);
    }

    /**
     * Updates the directory map with the values of the provided map.
     *
     * @param json_map_object Map of server IDs to locations
     */
    public void updateDirectoryMapping(JSONObject json_map_object) {

        update_lock.lockReadWrite();

        this.location_map.putAll(json_map_object);

        update_lock.unlockReadWrite();
    }

    /**
     * Returns a copy of the location map.
     *
     * @return Copy of the location map
     */
    public HashMap<String, String> getServerCollection() {

        update_lock.lockRead();

        HashMap<String, String> map_copy = new HashMap<String, String>(this.location_map);

        update_lock.unlockRead();

        return map_copy;
    }

    /**
     * Puts or updates a given server ID with the given location.
     *
     * @param id String ID of the server to be added
     * @param location Location of the server to be added
     */
    public void addServer(String id, String location) {

        update_lock.lockReadWrite();

        this.location_map.put(id, location);
        //update liveness_map

        update_lock.unlockReadWrite();
    }

    /**
     * Returns the location of the server with the given server ID.
     *
     * @param id String ID of the server to be located
     * @return Location of the specified server or null
     */
    public String getServerLocation(String id) {

        String location = null;

        if (id == null) {

            return location;
        }

        update_lock.lockRead();

        location = this.location_map.get(id);

        update_lock.unlockRead();

        return location;
    }

    /**
     * Sets the location to null for the server with the given server ID.
     *
     * @param server_id String ID of the server to be updated
     */
    public void setServerFailed(String server_id) {

        update_lock.lockReadWrite();

        location_map.put(server_id, null);

        update_lock.unlockReadWrite();
    }

    /**
     * Returns a JSONObject copy of the location map.
     *
     * @return JSONObject copy of the location map
     */
    public JSONObject toJSONObject() {

        update_lock.lockRead();

        JSONObject json_copy = new JSONObject(this.location_map);

        update_lock.unlockRead();

        return json_copy;
    }
}
