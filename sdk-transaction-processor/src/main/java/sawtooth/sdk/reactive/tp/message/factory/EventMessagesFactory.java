package sawtooth.sdk.reactive.tp.message.factory;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Event;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpEventAddRequest;
import sawtooth.sdk.protobuf.TpEventAddResponse;
import sawtooth.sdk.reactive.common.message.factory.AbstractMessageFactory;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * Some of the messages that circulate are basic to Sawtooth, not bound to any Transaction Processor
 * implementation.
 *
 */
public class EventMessagesFactory extends AbstractMessageFactory<Event> {

  protected EventMessagesFactory() throws NoSuchAlgorithmException {
    super();
  }

  private TpEventAddRequest createTpEventAddRequest(String contextId, String eventType,
      List<Event.Attribute> attributes, ByteString data) {

    TpEventAddRequest.Builder reqBuilder = TpEventAddRequest.newBuilder();

    Event.Builder eventBuilder = Event.newBuilder();
    eventBuilder.setData(data);
    eventBuilder.setEventType(eventType);
    eventBuilder.addAllAttributes(attributes);

    reqBuilder.setContextId(contextId);
    reqBuilder.setEvent(eventBuilder.build());

    return reqBuilder.build();
  }

  public TpEventAddResponse createTpEventAddResponse(Message respMesg)
      throws InvalidProtocolBufferException {
    TpEventAddResponse parsedExp = TpEventAddResponse.parseFrom(respMesg.getContent());
    return parsedExp;

  }

  public Message getEventAddRequest(String contextId, String eventType,
      List<Event.Attribute> attributes, ByteString data) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpEventAddRequest(contextId, eventType, attributes, data).toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_EVENT_ADD_REQUEST).build();

    return newMessage;
  }

}
