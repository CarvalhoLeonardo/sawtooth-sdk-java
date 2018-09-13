package sawtooth.sdk.reactive.tp.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.protobuf.InvalidProtocolBufferException;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.reactive.tp.fake.SimpleTestTransactionHandler;
import sawtooth.sdk.reactive.tp.processor.DefaultTransactionProcessorImpl;

@Test
public class TestExternalValidator extends BaseTest {
  /**
   * A tooled TransactionProcessor override to be tested.
   */
  private class TestTransactionProcessor extends DefaultTransactionProcessorImpl {
    public TestTransactionProcessor(String mqAddress, String tpId, int parallelismFactor) {
      super(mqAddress, tpId, parallelismFactor);
    }

    /**
     *
     * Let's ping ourselves, righ ?
     *
     * @return Ping response, if any
     */

    public Future<Message> ping() {
      Message pingReq;
      Future<Message> pingResp = null;
      try {
        pingReq = coreMessageFact.getPingRequest(null);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Pinging the Validator with {} ...", pingReq);
        }
        reactStream.send(pingReq);
        pingResp = reactStream.receive(pingReq.getCorrelationId());

      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return pingResp;
    }

    /**
     *
     * Let's send a Message.
     *
     * @return Future response.
     */

    public Future<Message> send(Message toSend) {
      Future<Message> response = null;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Sending message {} the Validator with CID {} ...", toSend.getContent(),
            toSend.getCorrelationId());
      }
      reactStream.send(toSend);
      response = reactStream.receive(toSend.getCorrelationId());

      return response;
    }

  }

  private static Properties configData = new Properties();

  private static final Logger LOGGER = LoggerFactory.getLogger(TestExternalValidator.class);
  /**
   * The TH under test.
   */
  private static final SimpleTestTransactionHandler testTH;
  static {
    SimpleTestTransactionHandler tmp = null;
    try {
      configData.load(Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("test_config.properties"));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    tmp = new SimpleTestTransactionHandler();
    testTH = tmp;
  }

  private TestTransactionProcessor tpUnderTest;



  @BeforeClass
  public void setUp() {
    tpUnderTest =
        new TestTransactionProcessor(
            configData.getProperty("validator_add") + ":"
                + configData.getProperty("validator_port"),
            configData.getProperty("tprocessor_id"),
            Integer.parseInt(configData.getProperty("parallelism")));
    try {
      tpUnderTest.init();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    tpUnderTest.addHandler(testTH);
    Assert.assertFalse(tpUnderTest.listRegisteredHandlers().isEmpty());
    Assert.assertEquals(tpUnderTest.listRegisteredHandlers().size(), 1);
    Assert.assertEquals(tpUnderTest.listRegisteredHandlers().get(0).getVersion(),
        testTH.getVersion());
  }

  /**
   * Let's send one ping.
   *
   * @throws InvalidProtocolBufferException
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws NoSuchAlgorithmException
   */
  @Test(enabled = false)
  public void testPing() throws InterruptedException, ExecutionException, NoSuchAlgorithmException {
    Message pong = tpUnderTest.ping().get();
    Assert.assertNotNull(pong);
    LOGGER.debug("Got a PING_RESPONSE with CID {}", pong.getCorrelationId());

  }

  /**
   *
   * This will send a Batch Request and expect a Batch Response.
   *
   * Since the External Validator probably don´t get a Handler, we will not test the procesing on
   * our side.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void testSendOneLameBatch()
      throws InterruptedException, ExecutionException, NoSuchAlgorithmException {
    ByteBuffer lameData = ByteBuffer.wrap("THIS IS A LAME PAYLOAD".getBytes());
    String lameAddress = "aaaaaaaaaaaaa";
    String correlationID = UUID.randomUUID().toString();

    Message preq = testTH.getMessageFactory().getProcessRequest(null, lameData,
        testTH.generateAddresses(testTH.getNameSpaces().iterator().next(), lameAddress),
        testTH.generateAddresses(testTH.getNameSpaces().iterator().next(), lameAddress), null,
        null);
    Batch batchReq = testTH.getMessageFactory().createBatch(Arrays.asList(preq), true);

    Message theReq =
        Message.newBuilder().setContent(batchReq.toByteString()).setCorrelationId(correlationID)
            .setMessageType(MessageType.CLIENT_BATCH_SUBMIT_REQUEST).build();
    Message answer = tpUnderTest.send(theReq).get();

    Assert.assertNotNull(answer);
    LOGGER.debug("Got the response {}", answer);

    Assert.assertEquals(answer.getCorrelationId(), correlationID);
    Assert.assertEquals(answer.getMessageType(), MessageType.CLIENT_BATCH_SUBMIT_RESPONSE);

  }
}
