package sawtooth.sdk.tp.tests;

import reactor.util.Logger;
import reactor.util.Loggers;

public abstract class BaseTest {
  protected final Logger LOGGER = Loggers.getLogger(this.getClass());
  
  static {
    // async logging
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());
    
    Loggers.useSl4jLoggers();
    
  }
}
