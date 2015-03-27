package main;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Parent class for both cache and central storage classes. This class manages
 * reading and writing to the storage data map.
 *
 * Created by luke on 9/27/14.
 */
public abstract class BaseStore {

    protected HashMap<String, HashMap<String, VectorTimestamp>> store_map;
    protected MultiReaderLock lock;

    public BaseStore(int max_concurrent_readers) {

        this.store_map = new HashMap<String, HashMap<String, VectorTimestamp>>();
        this.lock = new MultiReaderLock(max_concurrent_readers);
    }

    /**
     * Writes a value to each key in the list of keys.
     *
     * @param keys A list of keys
     * @param value Single value to be mapped to each key
     * @param timestamp VectorTimestamp to map to each value
     */
    protected void writeStore(List<String> keys, String value, VectorTimestamp timestamp) {

        for (String key : keys) {

            if (store_map.get(key) == null) {

                store_map.put(key, new HashMap<String, VectorTimestamp>());
            }
            store_map.get(key).put(value, timestamp);
        }
    }

    /**
     * Writes a map of values to a single key, appending the values to an
     * existing list if indicated.
     *
     * @param key A single key to which the values are mapped
     * @param values JSONObject of value mappings
     * @param append Indicates whether to append the values to an existing list
     */
    protected void writeStore(String key, JSONObject values, boolean append) {

        if (store_map.get(key) == null || !append) {

            HashMap<String, VectorTimestamp> value_set = new HashMap<String, VectorTimestamp>();

            store_map.put(key, value_set);
        }
        for (Map.Entry<String, JSONObject> entry : new HashMap<String, JSONObject>(values).entrySet()) {

            String store_entry = entry.getKey();
            VectorTimestamp timestamp = new VectorTimestamp(new HashMap<String, String>(entry.getValue()));

            if (store_map.get(key).get(store_entry) == null) {

                store_map.get(key).put(store_entry, timestamp);
            }
        }
    }

    /**
     * Reads the list of values mapped to the provided key.
     *
     * @param key Single key for which a mapped list is returned
     * @return A list of the mapped values
     */
    protected HashMap<String, VectorTimestamp> readStore(String key) {

        return store_map.get(key);
    }
}
