package main;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Data store memory class. This child class of BaseStore provides concurrency
 * control and data store entry version management.
 *
 * Created by luke on 9/28/14.
 */
public class DataStore extends BaseStore {

    private HashMap<String, String> version_numbers;
    private static DataStore singleton_data_store = null;

    private DataStore() {

        super(3);
        this.version_numbers = new HashMap<String, String>();
    }

    /**
     * Provides access to the singleton data store object for use across
     * multiple data store threads.
     *
     * @return An instance of the singleton data store
     */
    public static DataStore getInstance() {

        if (singleton_data_store == null) {

            singleton_data_store = new DataStore();
        }

        return singleton_data_store;
    }

    /**
     * Uses a lock to provide mutually exclusive writes to the data map.

     * @param hashtags List of keys to which the tweet value will be mapped
     * @param tweet Value to be mapped to each hashtag key
     * @param timestamp VectorTimestamp to be mapped to tweet
     */
    public void postToDataStore(List<String> hashtags, String tweet, VectorTimestamp timestamp) {

        lock.lockReadWrite();

        writeStore(hashtags, tweet, timestamp.copy());

        for (String hashtag : hashtags) {

            incrementVersionNumber(hashtag);
        }

        lock.unlockReadWrite();
    }

    /**
     * Uses a multi-read lock to provide concurrent reads of the data map.
     *
     * @param query Single query key to be read
     * @param version_number Version number to be compared
     * @param search_results List of results to be populated
     * @return Zero if no results, the current version number otherwise
     */
    public String searchDataStore(String query, String version_number, JSONObject search_results) {

        lock.lockRead();

        if (version_number.equals(getVersionNumber(query))) {

            lock.unlockRead();
        } else {

            HashMap<String, VectorTimestamp> results = readStore(query);
            if (results != null) {

                for (Map.Entry<String, VectorTimestamp> entry : results.entrySet()) {

                    search_results.put(entry.getKey(), entry.getValue().toJSONObject());
                }
            }

            version_number = getVersionNumber(query);
            lock.unlockRead();
        }

        if (version_number == null) {

            version_number = "0";
        }

        return version_number;
    }

    /**
     * Increments the version number for a given hashtag.
     *
     * @param hashtag Hashtag for which the version number is updated
     */
    private void incrementVersionNumber(String hashtag) {

        String version_number = this.getVersionNumber(hashtag);
        if (version_number != null) {

            this.version_numbers.put(hashtag, String.valueOf(Integer.decode(version_number) + 1));
        } else {

            this.version_numbers.put(hashtag, "1");
        }
    }

    /**
     * Monotonically updates the version number for a given hashtag to the given version number.
     *
     * @param hashtag Hashtag for which the version number is updated
     * @param new_version Replacement version number
     */
    private void updateVersionNumber(String hashtag, String new_version) {

        int version_number;
        String num_temp = this.getVersionNumber(hashtag);
        if (num_temp == null) {

            version_number = 0;
        } else {

            version_number = Integer.decode(num_temp);
        }

        if (version_number < Integer.decode(new_version)) {

            this.version_numbers.put(hashtag, new_version);
        }
    }

    /**
     * Retrieves the version number for the given hashtag.
     *
     * @param hashtag Hashtag for which the version number is requested
     * @return Version number for the given hashtag
     */
    private String getVersionNumber(String hashtag) {

        return version_numbers.get(hashtag);
    }

    /**
     * Merges this data store with another, writing all values from the latter
     * to the former. Additionally updates the version numbers to reflect all
     * changes resulting from the merge.
     *
     * @param data_map Map of the keys and values to be merged in
     * @param versions Map of the version numbers of the merge keys
     */
    public void mergeReplicationData(JSONObject data_map, JSONObject versions) {

        lock.lockReadWrite();

        HashMap<String, JSONObject> map_copy = new HashMap<String, JSONObject>(data_map);
        HashMap<String, String> version_map = new HashMap<String, String>(versions);
        for (Map.Entry<String, JSONObject> entry : map_copy.entrySet()) {

            String key = entry.getKey();
            writeStore(key, entry.getValue(), true);
            updateVersionNumber(key, version_map.get(key));
        }

        lock.unlockReadWrite();
    }

    /**
     * Builds a JSONObject that stores a copy of this data store's data map
     * and version map.
     *
     * @param min_stamp_value Minimum timestamp value allowed
     * @param max_stamp_value Maximum timestamp value allowed
     * @param server_id Target server ID for bounds determination
     * @return JSONObject containing copies of this data store's maps
     */
    public JSONObject getDataStoreCopy(int min_stamp_value, int max_stamp_value, String server_id) {

        lock.lockRead();

        JSONObject store_data_json_copy = new JSONObject();

        // Collect tweets for which the timestamp falls between the provided bounds with respect to the provided ID
        for (Map.Entry<String, HashMap<String, VectorTimestamp>> store_entry : this.store_map.entrySet()) {

            JSONObject tweet_mapping = new JSONObject();

            for (Map.Entry<String, VectorTimestamp> tweet_entry : store_entry.getValue().entrySet()) {

                Integer stamp_value = tweet_entry.getValue().getTimestamp(server_id);
                stamp_value = (stamp_value == null) ? 0 : stamp_value;
                if ((min_stamp_value == -1 && max_stamp_value == -1) || stamp_value > min_stamp_value && stamp_value <= max_stamp_value) {

                    tweet_mapping.put(tweet_entry.getKey(), tweet_entry.getValue().toJSONObject());
                }
            }

            store_data_json_copy.put(store_entry.getKey(), tweet_mapping);
        }

        JSONObject versions_copy = new JSONObject(this.version_numbers);

        JSONObject complete_store_copy = new JSONObject();
        complete_store_copy.put(Constants.Tokens.DATASTORE, store_data_json_copy);
        complete_store_copy.put(Constants.Tokens.VERSIONMAP, versions_copy);

        lock.unlockRead();

        return complete_store_copy;
    }

    /**
     * Builds a JSONObject that stores a copy of this data store's data map
     * and version map.
     *
     * @param upper_bound_timestamp Upper bound timestamp to compare against
     * @param ignore_id Server ID to be ignored during copy
     * @return JSONObject containing copies of this data store's maps
     */
    public JSONObject getDataStoreCopy(VectorTimestamp upper_bound_timestamp, String ignore_id) {

        lock.lockRead();

        JSONObject store_data_json_copy = new JSONObject();

        // Collect tweets for which the timestamp precedes the provided comparison timestamp
        for (Map.Entry<String, HashMap<String, VectorTimestamp>> store_entry : this.store_map.entrySet()) {

            JSONObject tweet_mapping = new JSONObject();

            for (Map.Entry<String, VectorTimestamp> tweet_entry : store_entry.getValue().entrySet()) {

                if (upper_bound_timestamp == null || tweet_entry.getValue().precedes(upper_bound_timestamp, ignore_id)) {

                    tweet_mapping.put(tweet_entry.getKey(), tweet_entry.getValue().toJSONObject());
                }
            }

            store_data_json_copy.put(store_entry.getKey(), tweet_mapping);
        }

        JSONObject versions_copy = new JSONObject(this.version_numbers);

        JSONObject complete_store_copy = new JSONObject();
        complete_store_copy.put(Constants.Tokens.DATASTORE, store_data_json_copy);
        complete_store_copy.put(Constants.Tokens.VERSIONMAP, versions_copy);

        lock.unlockRead();

        return complete_store_copy;
    }
}
