package sawtooth.sdk.tp.tests;

import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.common.utils.FormattingUtils;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchList;
import sawtooth.sdk.protobuf.ClientBatchStatusResponse;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.tp.fake.FakeValidator;
import sawtooth.sdk.tp.fake.SimpleTestTransactionHandler;
import sawtooth.sdk.tp.messaging.ReactorStream;
import sawtooth.sdk.tp.processor.DefaultTransactionProcessorImpl;

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
  private FakeValidator fVal = null;
  private ReactorStream reactStream = null;
  private ExecutorService tpe;
  private TestTransactionProcessor tpUnderTest;

  @BeforeClass
  public void setUp() {
    tpUnderTest = new TestTransactionProcessor(configData.getProperty("validator_add"),
        configData.getProperty("tprocessor_id"),
        Integer.parseInt(configData.getProperty("parallelism")));
    if ((configData.getProperty("really_external").isEmpty())
        || (configData.getProperty("really_external").equalsIgnoreCase("false"))) {
      LOGGER.info("Not an external Sawtooth Instance, let's start one fake validator...");
      fVal = new FakeValidator(testTH.getMessageFactory(), configData.getProperty("validator_add"));
      reactStream = new ReactorStream(configData.getProperty("validator_add"), 2);
      tpe = Executors.newWorkStealingPool(4);
      Future<?> startVal = tpe.submit(() -> {
        fVal.run();
      });
      try {
        startVal.get();
        LOGGER.debug("Preparing to start Stream...");
        Future<?> startStream = tpe.submit(() -> {
          reactStream.run();
        });
        startStream.get();
        LOGGER.debug("Stream Started.");
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        fail("Fake validator didn't go up !", e);
      }
      LOGGER.info("Fake validator started.");
    }

    try {
      tpUnderTest.init();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      fail("Test Transaction Processor didn't go up !", e);
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
   * DISABLED - we don't haev any motive to ping the Validator. Yet.
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
   * Since the External Validator probably don´t get a Handler, we will not test the processing on
   * our side.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws NoSuchAlgorithmException
   * @throws InvalidProtocolBufferException
   */
  @Test
  public void testSendOneLameBatch() throws InterruptedException, ExecutionException,
      NoSuchAlgorithmException, InvalidProtocolBufferException {
    ByteBuffer lameData = ByteBuffer.wrap("THIS IS A LAME PAYLOAD".getBytes());
    List<String> lameAddress = Arrays
        .asList(testTH.generateAddress(testTH.getNameSpaces().iterator().next(), "aaaaaaaaaaaa"));
    String correlationID = UUID.randomUUID().toString();
    Message lameProcessRequest = testTH.getMessageFactory().getProcessRequest(
        FormattingUtils.bytesToHex(testTH.getExternalContextID()), lameData, lameAddress,
        lameAddress, null, testTH.getMessageFactory().getPubliceyString());

    Batch lameBatchRequest = testTH.getMessageFactory()
        .createBatch(Arrays.asList(lameProcessRequest), true);
    /*
     * Yeah, you need to send a BatchList, NOT a Batch here...
     */
    BatchList.Builder rbl = BatchList.newBuilder();
    rbl.addBatches(lameBatchRequest);

    Message theReq = Message.newBuilder().setContent(rbl.build().toByteString())
        .setCorrelationId(correlationID).setMessageType(MessageType.CLIENT_BATCH_SUBMIT_REQUEST)
        .build();

    Message answer = tpUnderTest.send(theReq).get();

    Assert.assertNotNull(answer);

    ClientBatchStatusResponse responsePayload = ClientBatchStatusResponse.newBuilder()
        .mergeFrom(answer.getContent()).build();

    LOGGER.debug("Got the response {} with the payload {}.", answer, responsePayload);

    Assert.assertEquals(answer.getCorrelationId(), correlationID,
        "We got the wrong correlation id.");
    Assert.assertEquals(answer.getMessageType(), MessageType.CLIENT_BATCH_SUBMIT_RESPONSE,
        "We didn't receive the correct message type.");

    Assert.assertEquals(responsePayload.getStatus(), ClientBatchStatusResponse.Status.OK,
        "The transaction didn't succeed.");
  }
}
