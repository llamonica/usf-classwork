package main;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Front end cache class. This child class of BaseStore provides concurrency
 * control and cache entry version management.
 *
 * Created by luke on 9/27/14.
 */
public class CacheStore extends BaseStore {

    private HashMap<String, String> version_numbers;
    private static CacheStore singleton_cache_store = null;

    private CacheStore() {

        super(10);
        this.version_numbers = new HashMap<String, String>();
    }

    /**
     * Provides access to the singleton cache object for use across multiple
     * front end threads.
     *
     * @return An instance of the singleton cache store
     */
    public static CacheStore getInstance() {

        if (singleton_cache_store == null) {

            singleton_cache_store = new CacheStore();
        }

        return singleton_cache_store;
    }

    /**
     * Uses a multi-read lock to provide concurrent reads of the data map.
     *
     * @param query Single query key to be read
     * @return List of values mapped to query
     */
    public JSONArray readCache(String query) {

        lock.lockRead();

        final HashMap<String, VectorTimestamp> results = readStore(query);

        JSONArray results_replacement = new JSONArray();

        if (results != null) {

            for (Map.Entry<String, VectorTimestamp> entry : results.entrySet()) {

                results_replacement.add(entry.getKey());
            }
        }

        // Sort the mapped results before returning them
        Collections.sort(results_replacement, new Comparator<String>() {
            @Override
            public int compare(String o, String o2) {
                return (results.get(o).precedes(results.get(o2), null) ? -1 : 1);
            }
        });

        lock.unlockRead();

        return results_replacement;
    }

    /**
     * Uses a lock to provide mutually exclusive writes to the data map.
     *
     * @param query Single query key to be written
     * @param results List of values to be written
     * @param version_number Update to the version number for query
     */
    public void writeCache(String query, JSONObject results, String version_number) {

        lock.lockReadWrite();

        updateVersionNumber(version_number, query);
        writeStore(query, results, false);

        lock.unlockReadWrite();
    }

    /**
     * Updates the version number for the given query. There are no locks in
     * this method, because the locking is managed in the calling methods.
     *
     * @param version_number New version number
     * @param query Query for which the version number is to be updated
     */
    private void updateVersionNumber(String version_number, String query) {

        this.version_numbers.put(query, version_number);
    }

    /**
     * Retrieves the version number for the given query. A multi-read lock is
     * used to provide concurrent access to multiple readers.
     *
     * @param query Query for which the version number is requested
     * @return Version number for the given query
     */
    public String getVersionNumber(String query) {

        lock.lockRead();

        String version_number_temp = version_numbers.get(query);
        if (version_number_temp == null) {

            version_number_temp = "0";
        }

        lock.unlockRead();

        return version_number_temp;
    }
}
