package sawtooth.sdk.listener.test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import sawtooth.sdk.listener.conf.Communication;

@Test
public class TestBasicCommunication {

  private static Properties configData = new Properties();
  private static String validatorAddress;
  static {
    try {
      configData.load(Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("test_config.properties"));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    validatorAddress = (String) configData.get("validator_add");
  }

  private Communication myComm;

  @BeforeClass
  public void setUp() throws NoSuchAlgorithmException {
    myComm = new Communication(validatorAddress, 2, "lala");
  }

  @Test
  public void testCommunication() {
    Assert.assertNotNull(myComm);
    Boolean result = myComm.subscribeTo(Communication.EVENT_TYPE_STATE_DELTA, "lala");
    Assert.assertNotNull(result);
  }

  @Test
  public void testListening() throws InterruptedException {
    Thread.sleep(60000);
  }

}
