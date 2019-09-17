package sawtooth.sdk.reactive.tp.tests;

import java.io.IOException;
import java.util.Properties;

import reactor.util.Logger;
import reactor.util.Loggers;

public abstract class BaseTest {
  protected static int parallelFactor;
  protected static Properties testConfigData = new Properties();
  static {
    // async logging
    System.setProperty(org.apache.logging.log4j.core.util.Constants.LOG4J_CONTEXT_SELECTOR,
        org.apache.logging.log4j.core.async.AsyncLoggerContextSelector.class.getName());

    Loggers.useSl4jLoggers();
    try {
      testConfigData.load(Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("test_config.properties"));
      parallelFactor = Integer.parseInt(testConfigData.getProperty("parallelism", "2"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  protected final Logger LOGGER = Loggers.getLogger(this.getClass());
}
