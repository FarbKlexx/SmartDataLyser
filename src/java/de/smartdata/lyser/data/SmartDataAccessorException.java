package de.smartdata.lyser.data;

/**
 * Indicates an error with accessing SmartData
 * 
 * @author Florian Fehring
 */
public class SmartDataAccessorException extends Exception {

    /**
     * Creates a new instance of <code>SmartDataAccessorException</code> without
     * detail message.
     */
    public SmartDataAccessorException() {
    }

    /**
     * Constructs an instance of <code>SmartDataAccessorException</code> with
     * the specified detail message.
     *
     * @param msg the detail message.
     */
    public SmartDataAccessorException(String msg) {
        super(msg);
    }
}
