package sawtooth.sdk.reactive.common.messaging;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Event;
import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.Message.MessageType;
import sawtooth.sdk.protobuf.TpEventAddRequest;
import sawtooth.sdk.protobuf.TpEventAddResponse;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpRegisterRequest;
import sawtooth.sdk.protobuf.TpRegisterResponse;
import sawtooth.sdk.protobuf.TpUnregisterRequest;
import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 */
public class MessageFactory extends CoreMessagesFactory {

  /**
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(MessageFactory.class);
  final String familyName;
  final String familyVersion;

  final Map<String, String> nameSpacesMap;

  private final ECKey signerPrivateKey;
  private final byte[] signerPublicKeyEncodedPointByte;
  private final String signerPublicKeyString;

  public String createBArraySignature(byte[] bArray) {
    return SawtoothSigner.signHexSequence(signerPrivateKey, bArray);
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

  public TpProcessRequest createTpProcessRequest(String contextId, ByteBuffer payload,
      List<String> inputs, List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException, InvalidProtocolBufferException {

    if (batcherPubKey == null || batcherPubKey.isEmpty()) {
      throw new InvalidProtocolBufferException("No batcher public key informed.");
    }

    TpProcessRequest.Builder reqBuilder = TpProcessRequest.newBuilder();

    String hexFormattedDigest = generateHASH512Hex(payload.array());

    if (contextId != null && !contextId.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Context id set: " + contextId);
      }
      reqBuilder.setContextId(contextId);
    }

    reqBuilder.setHeader(createTransactionHeader(hexFormattedDigest, inputs, outputs, dependencies,
        Boolean.TRUE, batcherPubKey));

    reqBuilder.setPayload(ByteString.copyFrom(payload.array()));

    reqBuilder.setSignature(createBArraySignature(reqBuilder.getHeader().toByteArray()));

    return reqBuilder.build();
  }

  private TpRegisterRequest createTpRegisterRequest() {
    TpRegisterRequest.Builder reqBuilder = TpRegisterRequest.newBuilder();

    reqBuilder.setFamily(familyName);
    reqBuilder.addAllNamespaces(nameSpacesMap.keySet());
    reqBuilder.setVersion(familyVersion);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format("Register request : Family %s, Namespaces %s, Version %s",
          familyName, Arrays.deepToString(nameSpacesMap.keySet().toArray()), familyVersion));
    }
    return reqBuilder.build();
  }

  private TpRegisterResponse createTpRegisterResponse(int status) {
    TpRegisterResponse.Builder reqBuilder = TpRegisterResponse.newBuilder();

    reqBuilder.setStatusValue(status);

    return reqBuilder.build();
  }

  private TpUnregisterRequest createTpUnregisterRequest() {
    TpUnregisterRequest request = TpUnregisterRequest.newBuilder().build();
    return request;
  }

  private String generateHASH512Hex(byte[] toHash) {
    MESSAGEDIGESTER_512.reset();
    MESSAGEDIGESTER_512.update(toHash, 0, toHash.length);
    return FormattingUtils.bytesToHex(MESSAGEDIGESTER_512.digest());
  }

  public Message getEventAddRequest(String contextId, String eventType,
      List<Event.Attribute> attributes, ByteString data) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpEventAddRequest(contextId, eventType, attributes, data).toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_EVENT_ADD_REQUEST).build();

    return newMessage;
  }

  public final String getFamilyName() {
    return familyName;
  }

  public final String getFamilyVersion() {
    return familyVersion;
  }

  public final Map<String, String> getNameSpaces() {
    return nameSpacesMap;
  }

  public Message getProcessRequest(String contextId, ByteBuffer payload, List<String> inputs,
      List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException, InvalidProtocolBufferException {
    if (batcherPubKey == null || batcherPubKey.isEmpty()) {
      throw new InvalidProtocolBufferException("No batcher public key informed.");
    } else {
      LOGGER.debug("Batcher Public Key : {}.", batcherPubKey);
    }
    Message newMessage = Message.newBuilder()
        .setContent(
            createTpProcessRequest(contextId, payload, inputs, outputs, dependencies, batcherPubKey)
                .toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_PROCESS_REQUEST).build();

    return newMessage;
  }

  public Message getProcessResponse(String correlationId, String statusMessage,
      sawtooth.sdk.protobuf.TpProcessResponse.Status status, ByteString extData) {
    TpProcessResponse.Builder resBuilder = TpProcessResponse.newBuilder();

    if (extData != null && !extData.isEmpty())
      resBuilder.setExtendedData(extData);

    resBuilder.setMessage(statusMessage);
    resBuilder.setStatus(status);

    Message newMessage = Message.newBuilder().setContent(resBuilder.build().toByteString())
        .setCorrelationId(correlationId).setMessageType(MessageType.TP_PROCESS_RESPONSE).build();

    return newMessage;
  }

  public Message getProcessResponse(String correlationId, TpProcessResponse originalResponse) {
    Message newMessage = Message.newBuilder().setContent(originalResponse.toByteString())
        .setCorrelationId(correlationId).setMessageType(MessageType.TP_PROCESS_RESPONSE).build();

    return newMessage;
  }

  public String getPubliceyString() {
    return signerPublicKeyString;
  }

  public Message getRegisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpRegisterRequest().toByteString())
        .setCorrelationId(this.generateId()).setMessageType(MessageType.TP_REGISTER_REQUEST)
        .build();

    return newMessage;
  }

  public Message getRegisterResponse(int status, String correlationId) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpRegisterResponse(status).toByteString()).setCorrelationId(correlationId)
        .setMessageType(MessageType.TP_REGISTER_RESPONSE).build();

    return newMessage;
  }

  public byte[] getSignerPublicKeyEncodedPointByte() {
    return signerPublicKeyEncodedPointByte;
  }

  public Message getUnregisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpUnregisterRequest().toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_UNREGISTER_REQUEST).build();

    return newMessage;
  }

  private TpProcessResponse parseTpProcessResponse(Message message)
      throws InvalidProtocolBufferException {
    TpProcessResponse responseMessage = TpProcessResponse.parseFrom(message.getContent());

    return responseMessage;
  }
}
