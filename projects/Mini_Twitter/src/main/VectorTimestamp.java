package main;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

/**
 * Data structure class for mapping time stamp values to server IDs.
 *
 * Created by luke on 10/28/14.
 */
public class VectorTimestamp {

    HashMap<String, String> vector_elements;
    MultiReaderLock update_lock;

    final static Logger logger = Logger.getLogger(VectorTimestamp.class);

    public VectorTimestamp(Map<String, String> mapping) {

        this.vector_elements = new HashMap<String, String>(mapping);
        this.update_lock = new MultiReaderLock(-1);
    }

    public VectorTimestamp(ServerDirectory server_directory, String server_id) {

        this.vector_elements = new HashMap<String, String>();
        this.update_lock = new MultiReaderLock(-1);
        if (server_directory == null) {

            this.vector_elements.put(server_id, "0");
        } else {

            for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

                if (entry.getKey().startsWith(Constants.Config.DATASTORE)) {

                    this.vector_elements.put(entry.getKey(), "0");
                }
            }
        }
    }

    /**
     * Returns the timestamp value for the given server ID.
     *
     * @param server_id String ID of server for which a timestamp is requested
     * @return Time stamp value for the given server ID
     */
    public Integer getTimestamp(String server_id) {

        Integer timestamp = null;

        update_lock.lockRead();

        if (this.vector_elements.get(server_id) != null) {

            timestamp = Integer.decode(this.vector_elements.get(server_id));
        }

        update_lock.unlockRead();

        return timestamp;
    }

    /**
     * Returns a copy of the time stamp map.
     *
     * @return Copy of the time stamp map.
     */
    public HashMap<String, String> getTimestampMap() {

        update_lock.lockRead();

        HashMap<String, String> map_copy = new HashMap<String, String>(this.vector_elements);

        update_lock.unlockRead();

        return map_copy;
    }

    /**
     * Increments the time stamp value for the given server ID by one.
     *
     * @param server_id Server string ID for which stamp value is incremented
     */
    public void incrementTimestamp(String server_id) {

        update_lock.lockReadWrite();

        int new_value = Integer.decode(this.vector_elements.get(server_id)) + 1;
        this.vector_elements.put(server_id, String.valueOf(new_value));

        update_lock.unlockReadWrite();
    }

    /**
     * Updates the values of the local timestamp map to those of the provided
     * map such that all values of the local map will be equal or greater than
     * those of the provided map.
     *
     * @param timestamp_update Map containing update values
     */
    public void updateTimestamp(HashMap<String, String> timestamp_update) {

        update_lock.lockReadWrite();

        for (Map.Entry<String, String> entry : timestamp_update.entrySet()) {

            Integer target_value = Integer.decode(entry.getValue());

            if (this.vector_elements.get(entry.getKey()) == null
                    || Integer.decode(this.vector_elements.get(entry.getKey())) < target_value) {

                this.vector_elements.put(entry.getKey(), entry.getValue());
            }
        }

        update_lock.unlockReadWrite();
    }

    /**
     * Updates the stamp value for the given server ID.
     *
     * @param server_id Server string ID for which stamp value is updated
     * @param value Update value
     */
    public void updateTimestamp(String server_id, String value) {

        update_lock.lockReadWrite();

        this.vector_elements.put(server_id, value);

        update_lock.unlockReadWrite();
    }

    /**
     * Returns the first server ID encountered in the provided map such that
     * the stamp value of the server ID in the provided map is greater than
     * that found in the local map.
     *
     * @param target_timestamp Comparison map to be searched
     * @param ignore_id Server ID to ignore during search
     * @return First server ID with lesser value than the provided
     */
    public LinkedList<String[]> getOutdatedServerID(VectorTimestamp target_timestamp, String ignore_id) {

        HashMap<String, String> target_map = target_timestamp.getTimestampMap();
        LinkedList<String[]> stale_timestamp_server_ids = new LinkedList<String[]>();

        update_lock.lockRead();
        for (Map.Entry<String, String> entry : target_map.entrySet()) {

            if (ignore_id.equals(entry.getKey())) {

                continue;
            }

            Integer target_value = Integer.decode(entry.getValue());
            if (target_value > 0 && (this.vector_elements.get(entry.getKey()) == null
                    || Integer.decode(this.vector_elements.get(entry.getKey())) < target_value)) {

                stale_timestamp_server_ids.add(new String[]{entry.getKey(),
                        entry.getValue(), this.vector_elements.get(entry.getKey())});
            }
        }

        update_lock.unlockRead();

        Collections.sort(stale_timestamp_server_ids, new Comparator<String[]>() {
            @Override
            public int compare(String[] strings, String[] strings2) {
                return Integer.decode(strings[1]) - Integer.decode(strings2[1]);
            }
        });

        return stale_timestamp_server_ids;
    }

    /**
     * Returns the server ID that has the lowest corresponding stamp value.
     *
     * @param ignore_id Server ID to ignore during search
     * @return Server ID with lowest corresponding stamp value
     */
    public String getIDOfLowestStamp(String ignore_id) {

        logger.debug("Looking for lowest stamp in: " + toString());

        update_lock.lockRead();

        int min = Integer.MAX_VALUE;
        String server_id = null;
        for (Map.Entry<String, String> entry : this.vector_elements.entrySet()) {

            int current = Integer.decode(entry.getValue());
            if (!entry.getKey().equals(ignore_id) && current < min) {

                min = current;
                server_id = entry.getKey();
            }
        }

        update_lock.unlockRead();

        return server_id;
    }

    /**
     * Initializes missing values in the timestamp. Missing values are
     * determined by the presence of a server ID in the server directory and
     * the absence of the same server ID in the timestamp. Initialized values
     * are set to zero.
     *
     * @param server_directory ServerDirectory for which stamps are generated
     */
    protected void fillMissingValues(ServerDirectory server_directory) {

        if (server_directory != null) {

            update_lock.lockReadWrite();

            for (Map.Entry<String, String> entry : server_directory.getServerCollection().entrySet()) {

                if (entry.getKey().startsWith(Constants.Config.DATASTORE)
                        && this.vector_elements.get(entry.getKey()) == null) {

                    this.vector_elements.put(entry.getKey(), "0");
                }
            }

            update_lock.unlockReadWrite();
        }
    }

    /**
     * Returns a JSONObject copy of the time stamp map.
     *
     * @return JSONObject copy of the time stamp map
     */
    public JSONObject toJSONObject() {

        return new JSONObject(this.getTimestampMap());
    }

    /**
     * Returns a JSONstring representation of the time stamp map.
     *
     * @return JSONstring representation of the time stamp map
     */
    @Override
    public String toString() {

        return toJSONObject().toJSONString();
    }

    /**
     * Copies the server time stamp mapping into a new VectorTimestamp.
     *
     * @return VectorTimestamp copy
     */
    public VectorTimestamp copy() {

        return new VectorTimestamp(this.vector_elements);
    }

    /**
     * Determines if this timestamp precedes another.
     *
     * @param comparison_timestamp Timestamp to be compared to
     * @param ignore_id Server ID to ignore during comparison
     * @return Boolean indicating precedence
     */
    public boolean precedes(VectorTimestamp comparison_timestamp, String ignore_id) {

        boolean precedes = true;
        for (Map.Entry<String, String> entry : this.vector_elements.entrySet()) {

            if (ignore_id != null && entry.getKey().equals(ignore_id)) {

                continue;
            }

            HashMap<String, String> comparison_map = comparison_timestamp.getTimestampMap();
            String server_id = entry.getKey();
            if (comparison_map.containsKey(server_id)) {

                if (Integer.decode(comparison_map.get(server_id)) < Integer.decode(entry.getValue())) {

                    precedes = false;
                }
            } else {

                if (Integer.decode(entry.getValue()) > 0) {

                    precedes = false;
                }
            }
        }

        return precedes;
    }
}
