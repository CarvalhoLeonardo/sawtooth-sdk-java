package sawtooth.sdk.reactive.tp.processor;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import reactor.core.publisher.EmitterProcessor;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.reactive.common.exceptions.InternalError;
import sawtooth.sdk.reactive.common.exceptions.InvalidTransactionException;
import sawtooth.sdk.reactive.common.messaging.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.messaging.ReactorStream;

public class DefaultTransactionProcessorImpl implements TransactionProcessor {

  private final static Logger LOGGER =
      LoggerFactory.getLogger(DefaultTransactionProcessorImpl.class);
  private final static String KEY_FORMAT = "%s | %s";

  private final String address;
  private final String processorID;
  private int pFactor = 4;
  private final Map<String, TransactionHandler> messagesRouter =
      new ConcurrentHashMap<String, TransactionHandler>();
  private final Deque<TransactionHandler> currentHandlers =
      new ConcurrentLinkedDeque<TransactionHandler>();

  TpProcessRequest.Builder tpProcessRequestBuilder = TpProcessRequest.newBuilder();
  TpProcessResponse.Builder tpProcessResponseBuilder = TpProcessResponse.newBuilder();
  TpRegisterResponse.Builder tpRegRespBuilder = TpRegisterResponse.newBuilder();
  ReactorStream reactStream;
  Thread streamTH;
  protected CoreMessagesFactory cmf;

  public DefaultTransactionProcessorImpl(String mqAddress, String tpId, int parallelismFactor) {
    address = mqAddress;
    processorID = tpId;
    pFactor = parallelismFactor;
    reactStream = new ReactorStream(address, pFactor);
    streamTH = new Thread(reactStream);
    try {
      cmf = new CoreMessagesFactory();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() throws InterruptedException, ExecutionException {
    streamTH.run();
    reactStream.getStarted().get();
    LOGGER.debug("Message Stream Started.");
    reactStream.setTransformationFunction(coreMessagesFunction);
  }

  @Override
  public void shutdown() {}

  @Override
  public String getTransactionProcessorId() {
    return processorID;
  }

  @Override
  public void addHandler(TransactionHandler handler) {
    LOGGER.debug("Registering Handler {} - v{} ...", handler.transactionFamilyName(),
        handler.getVersion());
    Message regMess = handler.getMessageFactory().getRegisterRequest();

    try {
      LOGGER.debug("Sending Register Request ...");
      reactStream.send(regMess).get();
      LOGGER.debug("Register Request sent, waiting response.");
      Message answer = reactStream.receive(regMess.getCorrelationId()).get();
      LOGGER.debug("Register Response received : {}.",
          tpRegRespBuilder.mergeFrom(answer.getContent()).build().getStatus());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    currentHandlers.add(handler);

    messagesRouter.put(
        String.format(KEY_FORMAT, handler.transactionFamilyName(), handler.getVersion()), handler);

  }

  @Override
  public List<TransactionHandler> listRegisteredHandlers() {
    return Arrays.asList(currentHandlers.toArray(new TransactionHandler[currentHandlers.size()]));
  }

  @Override
  public void disableHandler(String transactionFamilyName, String version) {
    // TODO Auto-generated method stub

  }

  private Function<Message, Message> coreMessagesFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message mt) {
      TransactionHeader header;
      Message result = mt;
      try {
        switch (mt.getMessageType()) {
          case PING_REQUEST:
            result = cmf.getPingResponse(mt.getCorrelationId());
            break;
          case TP_PROCESS_REQUEST:
            TpProcessRequest theRequest = tpProcessRequestBuilder.mergeFrom(mt.getContent()).build(); 
            header = theRequest.getHeader();
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "Sending Process Request with Correlation ID {} to Transaction Handler {} of version {}",
                  mt.getCorrelationId(), header.getFamilyName(), header.getFamilyVersion());
            }
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Request : {}", theRequest.toString());
            }
            // We don't consume or process the message to permit further verification down the line.
            messagesRouter
                .get(String.format(KEY_FORMAT, header.getFamilyName(), header.getFamilyVersion()))
                .executeProcessRequest(theRequest, internalState);
            tpProcessRequestBuilder.clear();
            break;
          case TP_PROCESS_RESPONSE:
            if (LOGGER.isDebugEnabled()) {
              TpProcessResponse theResponse = tpProcessResponseBuilder.mergeFrom(mt.getContent()).build();
              LOGGER.debug(
                  "Received Process Response with Correlation ID {} with status {} and message {}",
                  mt.getCorrelationId(), theResponse.getStatus(), theResponse.getMessage());
              tpProcessResponseBuilder.clear();
            }
          case PING_RESPONSE:
          case NETWORK_ACK:
          case NETWORK_CONNECT:
          case NETWORK_DISCONNECT:
          case TP_EVENT_ADD_REQUEST:
          case TP_EVENT_ADD_RESPONSE:
          case TP_RECEIPT_ADD_DATA_RESPONSE:
          case TP_REGISTER_RESPONSE:
          case TP_STATE_DELETE_RESPONSE:
          case TP_STATE_GET_RESPONSE:
          case TP_STATE_SET_RESPONSE:
          case TP_UNREGISTER_REQUEST:
          case TP_UNREGISTER_RESPONSE:
          case UNRECOGNIZED:
          default:
            LOGGER.debug("Ignoring message type {}, passing away.", mt.getMessageType());
            result = mt;
            break;
        }
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return result;
    }
  };

  protected ReactiveStateImpl internalState = new ReactiveStateImpl();
  
  private final class ReactiveStateImpl implements SawtoothState {

    @Override
    public Map<String, ByteString> getState(List<String> addresses) {
      Map<String, ByteString> result = null;
      Message getStateMessage = cmf.getStateRequest(addresses);
      try {
        reactStream.send(getStateMessage).get();
        Message expectedAnswer = reactStream.receive(getStateMessage.getCorrelationId()).get();
        result = cmf.getStateResponse(expectedAnswer);
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
      return result;
    }

    @Override
    public Collection<String> setState(List<Entry<String, ByteString>> addressValuePairs)
        throws InternalError, InvalidTransactionException {
      Message getStateMessage = cmf.getSetStateRequest(processorID, addressValuePairs);
      List<String> result = null;
      try {
        reactStream.send(getStateMessage).get();
        Message expectedAnswer = reactStream.receive(getStateMessage.getCorrelationId()).get();
        result = cmf.parseStateSetResponse(expectedAnswer);
      } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
      return result;
    }

    @Override
    public ByteString AddEvent(String eventType, Map<String, String> attributes,
        ByteString extraData)
        throws InternalError, InvalidTransactionException, InvalidProtocolBufferException {
     throw new InternalError("Not Yet Implemented.");
    }

  }
}
