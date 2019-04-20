package sawtooth.sdk.listener.conf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.protobuf.InvalidProtocolBufferException;

import reactor.util.Logger;
import reactor.util.Loggers;
import sawtooth.sdk.protobuf.ClientEventsSubscribeRequest;
import sawtooth.sdk.protobuf.ClientEventsSubscribeResponse;
import sawtooth.sdk.protobuf.EventFilter;
import sawtooth.sdk.protobuf.EventFilter.FilterType;
import sawtooth.sdk.protobuf.EventSubscription;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;
import sawtooth.sdk.reactive.common.zmq.ReactorNetworkNode;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
@formatter:off
 * intkey create_batch -K 10 -B 20 -c 100
 * intkey load -f batches.intkey -v -U http://sawtooth-rest-api-default:8008
@formatter:on
 */

public class Communication {

  public static final String EVENT_TYPE_BLOCK_COMMIT = "sawtooth/block-commit";
  public static final String EVENT_TYPE_STATE_DELTA = "sawtooth/state-delta";
  private final static Logger LOGGER = Loggers.getLogger(Communication.class);
  private ReactorNetworkNode connectionNode;
  private Function<Message, Message> listenFunction = new Function<Message, Message>() {
    @Override
    public Message apply(Message m) {
      switch (m.getMessageTypeValue()) {
      case Message.MessageType.CLIENT_EVENTS_VALUE:
        LOGGER.debug("--------------------- Receiving EVENT : {}", m.getCorrelationId());
        break;
      default:
        LOGGER.debug("--------------------- Receiving INVALID EVENT : {}", m.getMessageType());
      }
      LOGGER.debug("answering with {}...", m.toString());
      return m;
    }
  };
  MessageDigest MESSAGEDIGESTER_512 = null;

  private ExecutorService tpe;

  @SuppressWarnings("unused")
  private Communication() {
  }

  public Communication(String mqAddress, int parallelismFactor, String name)
      throws NoSuchAlgorithmException {
    this.connectionNode = new ReactorNetworkNode(mqAddress, parallelismFactor, name, false);
    MESSAGEDIGESTER_512 = MessageDigest.getInstance("SHA-512");
    this.connectionNode.setWorkingFunction(listenFunction);
    tpe = Executors.newWorkStealingPool(4);

    LOGGER.debug("Preparing to start Node ...");
    Future<?> startVal = tpe.submit(() -> {
      this.connectionNode.run();
    });

    try {
      startVal.get();
      LOGGER.debug("Node Started.");
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

  }

  /**
   * generate a random String using the sha-512 algorithm, to correlate sent messages with futures
   *
   * Being random, we dont need to reset() it
   *
   * @return a random String
   */
  protected String generateId() {
    return FormattingUtils
        .bytesToHex(MESSAGEDIGESTER_512.digest(UUID.randomUUID().toString().getBytes()))
        .substring(0, 22);
  }

  public boolean subscribeTo(String type, String address) {
    ClientEventsSubscribeRequest.Builder eventSubReq = ClientEventsSubscribeRequest.newBuilder();

    EventSubscription.Builder evntSub = EventSubscription.newBuilder();
    evntSub.setEventType(EVENT_TYPE_STATE_DELTA);
    EventFilter.Builder evntFilter = EventFilter.newBuilder();
    evntFilter.setFilterType(FilterType.SIMPLE_ALL);
    evntSub.addFilters(evntFilter.build());
    eventSubReq.addSubscriptions(evntSub.build());
    String correlId = this.generateId();
    LOGGER.debug("Generated correlation id {} for subscription message ...", correlId);
    Message newMessage = Message.newBuilder().setContent(eventSubReq.build().toByteString())
        .setCorrelationId(correlId).setMessageType(MessageType.CLIENT_EVENTS_SUBSCRIBE_REQUEST)
        .build();

    LOGGER.debug("Sending message with correlation id {} ...", correlId);
    connectionNode.sendMessage(newMessage);
    LOGGER.debug("Sent !");
    Message respMesg = null;
    try {
      respMesg = connectionNode.waitForMessage(correlId).get(1000L, TimeUnit.SECONDS);
      LOGGER.debug("Answer received : {}.", respMesg.getMessageType());
      if (!(MessageType.CLIENT_EVENTS_SUBSCRIBE_RESPONSE == respMesg.getMessageType())) {
      } else {
        ClientEventsSubscribeResponse response = ClientEventsSubscribeResponse
            .parseFrom(respMesg.toByteArray());
        if (response != null
            && (ClientEventsSubscribeResponse.Status.OK.equals(response.getStatus()))) {
          return true;
        }
      }

    } catch (InterruptedException | ExecutionException | TimeoutException
        | InvalidProtocolBufferException e1) {
      e1.printStackTrace();
    }

    return false;
  }

}
