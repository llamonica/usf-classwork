package main;

/**
 * Exception used for parsing a HTTP request line.
 *
 * Created by luke on 9/17/14.
 */
public class HTTPRequestException extends Exception {

    public HTTPRequestException(String message) {

        super(message);
    }
}
