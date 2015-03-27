package main;

/**
 * Constant values used throughout the system code base.
 *
 * Created by luke on 9/27/14.
 */
public class Constants {

    public class Codes {

        public static final String OK = "200";
        public static final String CREATED = "201";
        public static final String NOTMODIFIED = "304";
        public static final String BADREQUEST = "400";
        public static final String NOTFOUND = "404";
        public static final String INTERNALSERVERERROR = "500";
    }

    public class Messages {

        public static final String OK = "OK";
        public static final String CREATED = "Created";
        public static final String NOTMODIFIED = "Not Modified";
        public static final String BADREQUEST = "Bad Request";
        public static final String NOTFOUND = "Not Found";
        public static final String INTERNALSERVERERROR = "Internal Server Error";
        public static final String BADJSON = "Invalid JSON-encoded body";
        public static final String BADPATH = "Unsupported URI path";
        public static final String BADMETHOD = "Unsupported HTTP method";
        public static final String INTERRUPTED = "Thread interrupted";
    }

    public class Config {

        public static final String DATASTORE = "datastore";
        public static final String FRONTEND = "frontend";
        public static final String DISCOVERY = "discovery";
        public static final String DEV = "dev";
        public static final String PRODUCTION = "production";
        public static final String IP = "ip";
        public static final String PORTS = "ports";
        public static final String LOCS = "locations";
        public static final String DELAY = "delayed";
    }

    public class Tokens {

        public static final String TWEETS = "tweets";
        public static final String TWEET = "tweet";
        public static final String HASH = "hashtags";
        public static final String QUERY = "q";
        public static final String VERSIONNUM = "v";
        public static final String HTTPVERSION = "HTTP/1.1";
        public static final String CONTENTLENGTH = "content-length";
        public static final String STATUS = "status";
        public static final String DISCOVER = "discover";
        public static final String SERVERID = "server_id";
        public static final String SERVERLOC = "server_loc";
        public static final String REPLICATE = "replicate";
        public static final String SERVERS = "servers";
        public static final String TIMESTAMP = "timestamp";
        public static final String STAMPS = "stamp_vals";
        public static final String DATASTORE = "datastore";
        public static final String VERSIONMAP = "version_map";
        public static final String LAYER = "layer";
        public static final String SNAPSHOT = "snapshot";
    }

    public class Discovery {

        public static final int TIMEOUT = 5000;
        public static final String SERVERS = "servers";
        public static final int LATENCYUPBOUND = 1000;
    }
}
