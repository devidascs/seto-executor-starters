package org.seto.commons.executor.error;

/**
 * Created by $3T0 Corp<br/>
 * User: devid<br/>
 * Date and Time: 3/22/2021 2:05 PM<br/>
 * <p> Description of SequentialExecutorError </p>
 */
public class SequentialExecutorError extends Error {
    private static final long serialVersionUID = 1L;

    public SequentialExecutorError(String message) {
        super(message);
    }
}
