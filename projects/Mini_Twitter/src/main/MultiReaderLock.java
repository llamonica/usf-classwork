package main;

/**
 * The main.MultiReaderLock manages concurrent reading operations and exclusive
 * writing.
 * 
 * @author Luke Lamonica
 */
public class MultiReaderLock {

	private int readers;
	private int writers;
    private int max_readers;
    private boolean multi_read;

	public MultiReaderLock(int max_readers) {

		this.readers = 0;
		this.writers = 0;
        this.multi_read = true;

        if (max_readers == -1) {

            max_readers = Integer.MAX_VALUE;
        }
        this.max_readers = max_readers;
	}

	/**
	 * Acquires a lock for reading so long as there are no active writers.
	 */
	public synchronized void lockRead() {

	    while (writers != 0 || !multi_read) {

	        try {

                wait();
            } catch (InterruptedException e) {

                System.out.println("Interrupted, try again.");
            }
	    }
        readers++;
        if (readers == max_readers) {

            multi_read = true;
        }
	}

	/**
	 * Releases the read lock and wakes threads. Threads waiting to write will
	 * now be able to proceed.
	 */
	public synchronized void unlockRead() {

        if (!multi_read && readers == 1) {

            multi_read = true;
        }
        readers--;
        notifyAll();
	}

	/**
	 * Acquires a lock for writing so long as there are no active readers and 
	 * no active writers.
	 */
	public synchronized void lockReadWrite() {

        while (readers != 0 || writers != 0) {

            try {

                wait();
            } catch (InterruptedException e) {

                System.out.println("Interrupted, try again.");
            }
        }
        writers++;
	}

	/**
	 * Releases the read/write lock and wakes threads. Threads waiting to
	 * either read or write will be able to proceed.
	 */
	public synchronized void unlockReadWrite() {

        writers--;
        notifyAll();
	}

	/**
	 * Retrieves the number of active readers.
	 * 
	 * @return Integer representing the number of active readers.
	 */
	public synchronized int numReaders() {
		
	    return readers;
	}

	/**
	 * Retrieves the number of active writers.
	 * 
	 * @return Integer representing the number of active writers.
	 */
	public synchronized int numWriters() {
		
	    return writers;
	}
}
