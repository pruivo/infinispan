package org.infinispan.util.concurrent;


public class ReadLockTimeoutException extends TimeoutException{

    public ReadLockTimeoutException(TimeoutException t){
        super(t.getMessage(),t.getCause());
    }

}
