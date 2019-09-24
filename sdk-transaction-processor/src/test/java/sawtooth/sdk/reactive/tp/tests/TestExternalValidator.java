package sawtooth.sdk.reactive.tp.tests;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchList;
import sawtooth.sdk.protobuf.ClientBatchStatusResponse;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.reactive.tp.fake.FakeValidator;
import sawtooth.sdk.reactive.tp.processor.DefaultTransactionProcessorImpl;
import sawtooth.sdk.reactive.tp.simulator.SimpleTestTransactionHandler;

@Test
public class TestExternalValidator extends BaseTest {

  /**
   * A tooled TransactionProcessor override to be tested.
   */
  private class TestTransactionProcessor extends DefaultTransactionProcessorImpl {
    public TestTransactionProcessor(String mqAddress, String tpId, int parallelismFactor,
        int timeoutMill) {
      super(mqAddress, tpId, parallelismFactor, timeoutMill);
    }

    /**
     *
     * Let's ping ourselves, right ?
     *
     * @return Ping response, if any
     */

    public Future<Message> ping() {

      Message pingReq;
      Future<Message> pingResp = null;
      try {
        pingReq = coreMessageFact.getPingRequest();
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
     * @return Future response.int parallelismFactor =
     * Integer.parseInt(configData.getProperty("parallelism"));
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

  private static final Logger LOGGER = LoggerFactory.getLogger(TestExternalValidator.class);

  /**
   * The TH under test.
   */
  private static final SimpleTestTransactionHandler testTH;
  static {

    SimpleTestTransactionHandler tmp = null;

    tmp = new SimpleTestTransactionHandler();
    /********************/
    // tmp.setContextId(UUID.randomUUID().toString().getBytes());
    /*****************/
    testTH = tmp;
  }
  FakeValidator faveValidatorForClosedTests;

  int timeout = 500;
  ExecutorService tpe = null;
  private TestTransactionProcessor tpUnderTest;

  @BeforeClass
  public void setUp() {
    try {
      if (testConfigData.get("localonly").toString().equalsIgnoreCase("true") || testConfigData
          .getProperty("validator_add").toString().equalsIgnoreCase("tcp://127.0.0.1:4004")) {
        // we will use the fake server
        tpe = Executors.newFixedThreadPool(2);
        faveValidatorForClosedTests = new FakeValidator(testTH, "tcp://127.0.0.1:4004",
            parallelFactor);
        LOGGER.debug("Preparing to start FAKE Validator...");
        tpe.submit(faveValidatorForClosedTests).get();
        LOGGER.debug("Validator Started.");
        LOGGER.debug("FAKE server Set up!");
      }
      tpUnderTest = new TestTransactionProcessor(testConfigData.getProperty("validator_add"),
          testConfigData.getProperty("tprocessor_id"), parallelFactor, timeout);
      tpUnderTest.init();
      LOGGER.debug("Transaction Processor Started!");
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    tpUnderTest.addHandler(testTH);
    LOGGER.debug("Transaction Handler Added!");
    Assert.assertFalse(tpUnderTest.listRegisteredHandlers().isEmpty());
    Assert.assertEquals(tpUnderTest.listRegisteredHandlers().size(), 1);
    Assert.assertEquals(
        tpUnderTest.listRegisteredHandlers().get(0).getTransactionFamily().getFamilyVersion(),
        testTH.getTransactionFamily().getFamilyVersion());
  }

  /**
   *
   * This will send a hundred Batch Request and expect the Batch Responses.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws NoSuchAlgorithmException
   * @throws InvalidProtocolBufferException
   */
  /*
   * @Test public void testSendHundredLameBatches() throws InterruptedException, ExecutionException,
   * NoSuchAlgorithmException, InvalidProtocolBufferException { ByteBuffer lameData =
   * ByteBuffer.wrap("THIS IS A LAME PAYLOAD".getBytes()); String testAddress =
   * testTH.getNameSpaces().iterator().next(); Map<String, Future<Message>> results = new
   * HashMap<>(); Flux<Boolean> messageReceiver = Flux.<Boolean>create(receiver -> { receiver. });
   *
   * for (int i = 0; i < 99; i++) { List<String> lameAddress = Arrays.asList(
   * testTH.generateAddress(testTH.getNameSpaces().iterator().next(), String.valueOf(i))); String
   * correlationID = UUID.randomUUID().toString();
   *
   * Message lameProcessRequest = testTH.getMessageFactory().getProcessRequest(
   * FormattingUtils.bytesToHex(testTH.getExternalContextID()), lameData, lameAddress, lameAddress,
   * null, testTH.getMessageFactory().getPubliceyString());
   *
   * Batch lameBatchRequest = testTH.getMessageFactory()
   * .createBatch(Arrays.asList(lameProcessRequest), true);
   *
   *
   * BatchList.Builder rbl = BatchList.newBuilder();rbl.addBatches(lameBatchRequest);
   *
   * Message theReq = Message.newBuilder().setContent(rbl.build().toByteString())
   * .setCorrelationId(correlationID).setMessageType(MessageType.CLIENT_BATCH_SUBMIT_REQUEST)
   * .build();
   *
   * results.put(correlationID,tpUnderTest.send(theReq));
   *
   * ClientBatchStatusResponse responsePayload = ClientBatchStatusResponse.newBuilder()
   * .mergeFrom(answer.getContent()).build();
   *
   * LOGGER.debug("Got the response {} with the payload {}.",answer,responsePayload);
   *
   * Assert.assertEquals(answer.getCorrelationId(),correlationID,"We got the wrong correlation id.")
   * ;Assert.assertEquals(answer.getMessageType(),MessageType.
   * CLIENT_BATCH_SUBMIT_RESPONSE,"We didn't receive the correct message type.");
   *
   * Assert.assertEquals(responsePayload.getStatus(),ClientBatchStatusResponse.Status.
   * OK,"The transaction didn't succeed."); }}
   */

  /**
   *
   * This will send a Batch Request and expect a Batch Response.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws NoSuchAlgorithmException
   * @throws InvalidProtocolBufferException
   */
  @Test
  public void testSendOneLameBatch() throws InterruptedException, ExecutionException,
      NoSuchAlgorithmException, InvalidProtocolBufferException {
    ByteBuffer lameData = ByteBuffer
        .wrap(("THIS IS A LAME PAYLOAD [" + UUID.randomUUID().toString() + "]").getBytes());
    List<String> lameAddress = Arrays.asList(testTH.getTransactionFamily()
        .generateAddress(testTH.getNameSpaces().iterator().next(), "aaaaaaaaaaaa"));
    String correlationID = UUID.randomUUID().toString();

    Transaction lameProcessRequest = testTH.getTransactionFactory().createTransaction(lameData,
        lameAddress, lameAddress, Collections.emptyList(), null);

    Batch lameBatchRequest = testTH.getBatchFactory().createBatch(Arrays.asList(lameProcessRequest),
        true);

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
