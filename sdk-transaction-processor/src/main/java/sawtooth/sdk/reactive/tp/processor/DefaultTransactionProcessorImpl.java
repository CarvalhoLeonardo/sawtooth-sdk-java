package sawtooth.sdk.reactive.tp.processor;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpProcessResponse.Status;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.factory.FamilyRegistryMessageFactory;
import sawtooth.sdk.reactive.tp.message.flow.ReactorStream;

public class DefaultTransactionProcessorImpl implements TransactionProcessor {
  private final static String KEY_FORMAT = "%s | %s";

  private final static Logger LOGGER = LoggerFactory
      .getLogger(DefaultTransactionProcessorImpl.class);
  private final String address;

  protected CoreMessagesFactory coreMessageFact;
  private Function<Message, Message> coreMessagesFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message mt) {
      Message result = mt;
      try {
        switch (mt.getMessageType()) {
        case PING_REQUEST:
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ping Request with Correlation ID {} ...", mt.getCorrelationId());
          }
          result = coreMessageFact.getPingResponse(mt.getCorrelationId());
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Answered with {} ...", result.toString());
          }
          break;
        case TP_PROCESS_REQUEST:

          // This message will be "transformed" to a TP_PROCESS_RESPONSE

          tasksExecutor.submit(new Callable<TpProcessResponse>() {

            @Override
            public TpProcessResponse call() throws Exception {
              TpProcessRequest theRequest = tpProcessRequestBuilder.mergeFrom(mt.getContent())
                  .build();

              TransactionHeader header = theRequest.getHeader();
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Sending Process Request with Correlation ID {} to Transaction Handler {} of version {}",
                    mt.getCorrelationId(), header.getFamilyName(), header.getFamilyVersion());
              }
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Request : {}", theRequest.toString());
              }
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Parallel processing {} ...", mt.getCorrelationId());
                LOGGER.debug("Sending to {} ...",
                    String.format(KEY_FORMAT, header.getFamilyName(), header.getFamilyVersion()));
              }

              tpProcessRequestBuilder.clear();
              CompletableFuture<TpProcessResponse> waitinResp = messagesRouter
                  .get(String.format(KEY_FORMAT, header.getFamilyName(), header.getFamilyVersion()))
                  .apply(theRequest, internalState);

              waitinResp.thenAccept(rs -> {
                LOGGER.debug("Computation returned {} -- {}", rs.getStatus(), rs.getMessage());
                if (!rs.getStatus().equals(Status.OK)) {
                  LOGGER.error("Computation failed.");
                }
              });

              return waitinResp.get();
            }
          });

          break;
        case TP_PROCESS_RESPONSE:
          TpProcessResponse theResponse = tpProcessResponseBuilder.mergeFrom(mt.getContent())
              .build();
          LOGGER.debug(
              "Received Process Response with Correlation ID {} with status {} and message {}",
              mt.getCorrelationId(), theResponse.getStatus(), theResponse.getMessage());
          tpProcessResponseBuilder.clear();

          break;
        case TP_STATE_GET_RESPONSE:
        case TP_STATE_SET_RESPONSE:
          LOGGER.debug("Received State Manipulation message Correlation ID {} ",
              mt.getCorrelationId());

        case PING_RESPONSE:
        case NETWORK_ACK:
        case NETWORK_CONNECT:
        case NETWORK_DISCONNECT:
        case TP_EVENT_ADD_REQUEST:
        case TP_STATE_DELETE_RESPONSE:
        case TP_EVENT_ADD_RESPONSE:
        case TP_RECEIPT_ADD_DATA_RESPONSE:
        case TP_UNREGISTER_REQUEST:
        case TP_UNREGISTER_RESPONSE:
        case UNRECOGNIZED:
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Ignoring Message Type {} with Correlation ID {} due to lack of dealing method...",
                mt.getMessageType(), mt.getCorrelationId());
          }
          break;
        default:
          LOGGER.debug("Ignoring unexpected message type {}, passing away.", mt.getMessageType());
          break;
        }
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return result;
    }
  };

  private final Deque<TransactionHandler> currentHandlers = new ConcurrentLinkedDeque<TransactionHandler>();

  protected DefaultSawtoothStateImpl internalState;

  private final Map<String, TransactionHandler> messagesRouter = new ConcurrentHashMap<String, TransactionHandler>();

  private int pFactor;
  private final String processorID;
  protected ReactorStream reactStream;
  protected FamilyRegistryMessageFactory registryMessageFact;

  Thread streamTH;

  ExecutorService tasksExecutor;

  TpProcessRequest.Builder tpProcessRequestBuilder = TpProcessRequest.newBuilder();

  TpProcessResponse.Builder tpProcessResponseBuilder = TpProcessResponse.newBuilder();

  TpRegisterResponse.Builder tpRegRespBuilder = TpRegisterResponse.newBuilder();

  /**
   *
   * Create the instance
   *
   * @param mqAddress - MQ Address
   * @param tpId - Transaction Processor ID
   * @param parallelismFactor - Parallelism factor for the Streams
   * @param timeoutMilisseconds - timeout im millisseconds to the async operations
   */
  public DefaultTransactionProcessorImpl(String mqAddress, String tpId, int parallelismFactor,
      int timeoutMilisseconds) {
    address = mqAddress;
    processorID = tpId;
    pFactor = parallelismFactor;
    reactStream = new ReactorStream(address, pFactor);
    streamTH = new Thread(reactStream);
    tasksExecutor = Executors.newWorkStealingPool(parallelismFactor);
    try {
      coreMessageFact = new CoreMessagesFactory();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    internalState = new DefaultSawtoothStateImpl(reactStream, timeoutMilisseconds);
  }

  @Override
  public void addHandler(TransactionHandler handler) {
    LOGGER.debug("Registering Handler {} - v{} ...", handler.getTransactionFamily().getFamilyName(),
        handler.getTransactionFamily().getFamilyVersion());
    Message regMess = handler.getFamilyRegistryMessageFactory().generateRegisterRequest();

    try {
      LOGGER.debug("Sending Register Request ...");
      reactStream.send(regMess).get();
      LOGGER.debug("Register Request sent, waiting response.");
      Message answer = reactStream.receive(regMess.getCorrelationId()).get();
      LOGGER.debug("Register Response received : {}.",
          tpRegRespBuilder.mergeFrom(answer.getContent()).build());

    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    handler.setContextId(getExternalContextID());
    currentHandlers.add(handler);

    messagesRouter.put(String.format(KEY_FORMAT, handler.getTransactionFamily().getFamilyName(),
        handler.getTransactionFamily().getFamilyVersion()), handler);

  }

  @Override
  public void disableHandler(String transactionFamilyName, String version) {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getExternalContextID() {
    return reactStream.getExternalContext();
  }

  @Override
  public String getTransactionProcessorId() {
    return processorID;
  }

  @Override
  public void init() throws InterruptedException, ExecutionException {
    streamTH.run();
    reactStream.getStarted().get();
    LOGGER.debug("Message Stream Started.");
    reactStream.setTransformationFunction(coreMessagesFunction);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.debug("Signaled to stop all.");
      this.shutdown();
    }));

  }

  @Override
  public List<TransactionHandler> listRegisteredHandlers() {
    return Arrays.asList(currentHandlers.toArray(new TransactionHandler[currentHandlers.size()]));
  }

  @Override
  public void shutdown() {
    LOGGER.debug("Shutting down...");
    List<Message> goobByeLetters = new ArrayList<>();
    List<CompletableFuture<Message>> allToReceive = new ArrayList<>();

    currentHandlers.forEach(eh -> {
      LOGGER.debug("Getting unregister request for TH of family {}, version {}",
          eh.getTransactionFamily().getFamilyName(), eh.getTransactionFamily().getFamilyVersion());
      goobByeLetters.add(eh.getFamilyRegistryMessageFactory().generateUnregisterRequest());
    });

    goobByeLetters.stream().map(em -> {
      reactStream.sendBack(em.getCorrelationId(), em);
      try {
        CompletableFuture<Message> awaitingOne;

        awaitingOne = (CompletableFuture<Message>) reactStream.receive(em.getCorrelationId(),
            Duration.ofSeconds(1L));
        awaitingOne.whenComplete((rs, ex) -> {
          if (ex != null) {
            LOGGER.debug("::==========>> FAILURE {}.", ex.getMessage());
          } else {
            LOGGER.debug("Successfully unregistered : CID {}.", rs.getCorrelationId());
          }
        });
        allToReceive.add(awaitingOne);
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
      return true;
    }).allMatch(al -> true);

    CompletableFuture<Void> result = CompletableFuture
        .allOf(allToReceive.toArray(new CompletableFuture<?>[allToReceive.size()]));
    result.join();
    try {
      result.get(10, TimeUnit.SECONDS);
      reactStream.close();
      streamTH.join();

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      e.printStackTrace();
    }

  }

}
