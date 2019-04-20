package sawtooth.sdk.listener.test;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class TestsListener implements ISuiteListener, ITestListener {

  static final SeContainerInitializer initializer;
  private final static Logger LOGGER = LoggerFactory.getLogger(TestsListener.class);
  static {
    initializer = SeContainerInitializer.newInstance().disableDiscovery();
  }
  private SeContainer container;
  private boolean started = false;

  @Override
  public void onFinish(ISuite suite) {
    if (started) {
      // stopCDI();
    }
  }

  @Override
  public void onFinish(ITestContext context) {
    if (started) {
      // stopCDI();
    }

  }

  @Override
  public void onStart(ISuite suite) {
    if (!started) {
      startCDI();
    }
  }

  @Override
  public void onStart(ITestContext context) {
    if (!started) {
      startCDI();
    }

  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    // TODO Auto-generated method stub

  }

  @Override
  public void onTestFailure(ITestResult result) {
    // TODO Auto-generated method stub

  }

  @Override
  public void onTestSkipped(ITestResult result) {
    // TODO Auto-generated method stub

  }

  @Override
  public void onTestStart(ITestResult result) {
    if (!started) {
      startCDI();
    }

  }

  @Override
  public void onTestSuccess(ITestResult result) {
    // TODO Auto-generated method stub

  }

  private void startCDI() {
    LOGGER.info("Starting CDI ...");
    started = container.isRunning();
    LOGGER.info("Started : " + started);
  }

  private void stopCDI() {
    LOGGER.info("Stopping CDI ...");
    if (started) {
      container.close();
    }
    started = container.isRunning();
    LOGGER.info("Stopped : " + !started);
  }

}
