package sawtooth.sdk.reactive.common.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchHeader;
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
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.protobuf.TransactionList;
import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;

public class MessageFactory extends CoreMessagesFactory {

  final String[] nameSpaces;
  final String familyName;
  final String familyVersion;

  private final static Logger LOGGER = LoggerFactory.getLogger(MessageFactory.class);

  private final ECKey signerPrivateKey;
  private final String signerPublicKeyString;
  private final byte[] signerPublicKeyEncodedPointByte;

  @SuppressWarnings("unused")
  private MessageFactory() throws NoSuchAlgorithmException {
    super("");
    signerPrivateKey = null;
    signerPublicKeyString = null;
    signerPublicKeyEncodedPointByte = null;   
    nameSpaces = null;
    familyName = null;
    familyVersion = null;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }
  
  public MessageFactory(String familyName, String familyVersion, ECKey privateKey, ECKey publicKey,
      String... nameSpaces) throws NoSuchAlgorithmException {
    this(familyName, "SHA-512", familyVersion, privateKey, publicKey, nameSpaces);
  }

  public MessageFactory(String familyName, String digesterAlgo, String familyVersion,
      ECKey privateKey, ECKey publicKey, String... nameSpaces) throws NoSuchAlgorithmException {
    super(digesterAlgo);
    this.familyName = familyName;
    this.familyVersion = familyVersion;
    if (privateKey == null) {
      LOGGER.warn("Private Key null, creating a temporary one...");
      this.signerPrivateKey = SawtoothSigner.generatePrivateKey(new SecureRandom(ByteBuffer
          .allocate(Long.BYTES).putLong(Calendar.getInstance().getTimeInMillis()).array()));
      LOGGER.warn("Created with encryption " + this.signerPrivateKey.getEncryptionType().toString()
          + " and Key Crypter " + this.signerPrivateKey.getKeyCrypter());
    } else {
      this.signerPrivateKey = privateKey;
    }
    if (publicKey == null) {
      signerPublicKeyEncodedPointByte = signerPrivateKey.getPubKeyPoint().getEncoded(true);
      signerPublicKeyString = FormattingUtils.bytesToHex(signerPublicKeyEncodedPointByte);
    } else {
      signerPublicKeyEncodedPointByte = publicKey.getPubKeyPoint().getEncoded(true);
      signerPublicKeyString = FormattingUtils.bytesToHex(publicKey.getPubKey());
    }

    List<String> binNameSpaces = new ArrayList<String>();
    for (String eachNS : nameSpaces) {
      binNameSpaces
          .add(FormattingUtils.hash512(eachNS.getBytes(StandardCharsets.UTF_8)).substring(0, 6));
    }
    this.nameSpaces = new String[nameSpaces.length];
    binNameSpaces.toArray(this.nameSpaces);

  }

  public final String[] getNameSpaces() {
    return nameSpaces;
  }

  public final String getFamilyName() {
    return familyName;
  }

  public final String getFamilyVersion() {
    return familyVersion;
  }

  public String getPubliceyString() {
    return signerPublicKeyString;
  }

  public byte[] getSignerPublicKeyEncodedPointByte() {
    return signerPublicKeyEncodedPointByte;
  }


  public final TransactionHeader createTransactionHeader(String payloadSha512, List<String> inputs,
      List<String> outputs, List<String> dependencies, boolean needsNonce, String batcherPubKey) {
    TransactionHeader.Builder thBuilder = TransactionHeader.newBuilder();
    thBuilder.setFamilyName(familyName);
    thBuilder.setFamilyVersion(familyVersion);
    thBuilder.setSignerPublicKey(getPubliceyString());
    thBuilder.setBatcherPublicKey(
        batcherPubKey != null ? batcherPubKey : thBuilder.getSignerPublicKey());
    thBuilder.setPayloadSha512(payloadSha512);

    if (needsNonce) {
      thBuilder.setNonce(String.valueOf(Calendar.getInstance().getTimeInMillis()));
    }

    if (dependencies != null && !dependencies.isEmpty()) {
      thBuilder.addAllDependencies(dependencies);
    }

    if (inputs != null && !inputs.isEmpty()) {
      thBuilder.addAllInputs(inputs);
    }

    if (outputs != null && !outputs.isEmpty()) {
      thBuilder.addAllOutputs(outputs);
    }

    return thBuilder.build();
  }

  public String createHeaderSignature(TransactionHeader header) {
    return SawtoothSigner.signHexSequence(signerPrivateKey, header.toByteArray());
  }

  public Message getRegisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpRegisterRequest().toByteString())
        .setCorrelationId(this.generateId()).setMessageType(MessageType.TP_REGISTER_REQUEST)
        .build();

    return newMessage;
  }

  private TpRegisterRequest createTpRegisterRequest() {
    TpRegisterRequest.Builder reqBuilder = TpRegisterRequest.newBuilder();

    reqBuilder.setFamily(familyName);
    reqBuilder.addAllNamespaces(Arrays.asList(nameSpaces));
    reqBuilder.setVersion(familyVersion);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format("Register request : Family %s, Namespaces %s, Version %s",
          familyName, Arrays.deepToString(nameSpaces), familyVersion));
    }
    return reqBuilder.build();
  }

  public Message getRegisterResponse(int status, String correlationId) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpRegisterResponse(status).toByteString()).setCorrelationId(correlationId)
        .setMessageType(MessageType.TP_REGISTER_RESPONSE).build();

    return newMessage;
  }

  private TpRegisterResponse createTpRegisterResponse(int status) {
    TpRegisterResponse.Builder reqBuilder = TpRegisterResponse.newBuilder();

    reqBuilder.setStatusValue(status);

    return reqBuilder.build();
  }

  public Message getUnregisterRequest() {
    Message newMessage = Message.newBuilder().setContent(createTpUnregisterRequest().toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_UNREGISTER_REQUEST).build();

    return newMessage;
  }

  private TpUnregisterRequest createTpUnregisterRequest() {
    TpUnregisterRequest request = TpUnregisterRequest.newBuilder().build();
    return request;
  }

  

  public Message getProcessRequest(String contextId, ByteBuffer payload, List<String> inputs,
      List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException {
    Message newMessage =
        Message.newBuilder()
            .setContent(createTpProcessRequest(contextId, payload, inputs, outputs, dependencies,
                batcherPubKey).toByteString())
            .setCorrelationId(generateId()).setMessageType(MessageType.TP_PROCESS_REQUEST).build();

    return newMessage;
  }


  public TpProcessRequest createTpProcessRequest(String contextId, ByteBuffer payload,
      List<String> inputs, List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException {
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

    reqBuilder.setSignature(createHeaderSignature(reqBuilder.getHeader()));

    return reqBuilder.build();
  }

  public Message getProcessResponse(String correlationId, TpProcessResponse originalResponse) {
    Message newMessage = Message.newBuilder().setContent(originalResponse.toByteString())
        .setCorrelationId(correlationId).setMessageType(MessageType.TP_PROCESS_RESPONSE).build();

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

  private TpProcessResponse parseTpProcessResponse(Message message)
      throws InvalidProtocolBufferException {
    TpProcessResponse responseMessage = TpProcessResponse.parseFrom(message.getContent());

    return responseMessage;
  }

  

  public Message getEventAddRequest(String contextId, String eventType,
      List<Event.Attribute> attributes, ByteString data) {
    Message newMessage = Message.newBuilder()
        .setContent(createTpEventAddRequest(contextId, eventType, attributes, data).toByteString())
        .setCorrelationId(generateId()).setMessageType(MessageType.TP_EVENT_ADD_REQUEST).build();

    return newMessage;
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


  private Transaction createTransaction(ByteBuffer payload, List<String> inputs,
      List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException {
    Transaction.Builder transactionBuilder = Transaction.newBuilder();
    transactionBuilder
        .setPayload(ByteString.copyFrom(payload.toString(), StandardCharsets.US_ASCII));
    TransactionHeader header = createTransactionHeader(generateHASH512Hex(payload.array()), inputs,
        outputs, dependencies, Boolean.TRUE, batcherPubKey);
    transactionBuilder.setHeader(header.toByteString());
    transactionBuilder.setHeaderSignature(createHeaderSignature(header));

    return transactionBuilder.build();
  }

  private Transaction createTransactionFromProcessRequest(Message processRequest)
      throws InvalidProtocolBufferException {
    Transaction.Builder transactionBuilder = Transaction.newBuilder();
    TpProcessRequest theRequest = TpProcessRequest.parseFrom(processRequest.getContent());
    transactionBuilder.setHeader(theRequest.getHeader().toByteString());
    transactionBuilder.setPayload(theRequest.getPayload());
    String hexFormattedDigest = generateHASH512Hex(theRequest.getPayload().toByteArray());
    TransactionHeader header =
        createTransactionHeader(hexFormattedDigest, theRequest.getHeader().getInputsList(),
            theRequest.getHeader().getOutputsList(), theRequest.getHeader().getDependenciesList(),
            Boolean.TRUE, theRequest.getHeader().getBatcherPublicKey());
    transactionBuilder.setHeader(header.toByteString());
    transactionBuilder.setHeaderSignature(createHeaderSignature(header));

    return transactionBuilder.build();
  }


  public Batch createBatch(List<? extends Message> transactions, boolean trace) {


    TransactionList.Builder transactionListBuilder = TransactionList.newBuilder();

    List<String> txnSignatures = transactions.stream().map(et -> {
      String result = "";
      try {
        Transaction toAdd;
        if (et.getMessageType().equals(MessageType.TP_PROCESS_REQUEST)) {
          toAdd = createTransactionFromProcessRequest(et);

        } else {
          toAdd = Transaction.parseFrom(et.getContent());
        }
        transactionListBuilder.addTransactions(toAdd);
        result = toAdd.getHeaderSignature();
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error(
            "InvalidProtocolBufferException on Message " + et.toString() + " : " + e.getMessage());
        e.printStackTrace();
      }
      return result;
    }).collect(Collectors.toList());


    BatchHeader batchHeader = BatchHeader.newBuilder().addAllTransactionIds(txnSignatures)
        .setSignerPublicKey(getPubliceyString()).build();

    String headerSignature =
        SawtoothSigner.signHexSequence(signerPrivateKey, batchHeader.toByteArray());

    Batch.Builder batchBuilder =
        Batch.newBuilder().setHeader(batchHeader.toByteString()).setHeaderSignature(headerSignature)
            .addAllTransactions(transactionListBuilder.build().getTransactionsList());

    if (LOGGER.isTraceEnabled() || trace) {
      batchBuilder.setTrace(true);
    }

    return batchBuilder.build();
  }

  private String generateHASH512Hex(byte[] toHash) {
    MESSAGEDIGESTER_512.reset();
    MESSAGEDIGESTER_512.update(toHash, 0, toHash.length);
    return FormattingUtils.bytesToHex(MESSAGEDIGESTER_512.digest());
  }
}
