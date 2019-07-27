package sawtooth.sdk.reactive.common.message.factory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionHeader;

public class TransactionFactory {

  /**
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(TransactionFactory.class);

  private Transaction createTransaction(ByteBuffer payload, List<String> inputs,
      List<String> outputs, List<String> dependencies, String batcherPubKey)
      throws NoSuchAlgorithmException, InvalidProtocolBufferException {
    if (batcherPubKey == null || batcherPubKey.isEmpty()) {
      throw new InvalidProtocolBufferException("No batcher public key informed.");
    }
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
    TransactionHeader header = createTransactionHeader(hexFormattedDigest,
        theRequest.getHeader().getInputsList(), theRequest.getHeader().getOutputsList(),
        theRequest.getHeader().getDependenciesList(), Boolean.TRUE,
        theRequest.getHeader().getBatcherPublicKey());
    transactionBuilder.setHeader(header.toByteString());
    transactionBuilder.setHeaderSignature(createHeaderSignature(header));

    return transactionBuilder.build();
  }

  public final TransactionHeader createTransactionHeader(String payloadSha512, List<String> inputs,
      List<String> outputs, List<String> dependencies, boolean needsNonce, String batcherPubKey)
      throws InvalidProtocolBufferException {
    if (batcherPubKey == null || batcherPubKey.isEmpty()) {
      throw new InvalidProtocolBufferException("No batcher public key informed.");
    }
    TransactionHeader.Builder thBuilder = TransactionHeader.newBuilder();
    thBuilder.setFamilyName(familyName);
    thBuilder.setFamilyVersion(familyVersion);
    thBuilder.setSignerPublicKey(getPubliceyString());
    thBuilder.setBatcherPublicKey(batcherPubKey);
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
}
