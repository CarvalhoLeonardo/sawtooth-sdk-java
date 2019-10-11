package sawtooth.sdk.reactive.common.message.factory;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.List;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;
import sawtooth.sdk.reactive.common.family.TransactionFamily;

public class TransactionFactory extends AbstractFamilyMessagesFactory<Transaction> {

  /**
   * Our ubiquitous Logger.
   */
  private final static Logger LOGGER = LoggerFactory.getLogger(TransactionFactory.class);

  final TransactionFamily messagesTFamily;

  /**
   *
   * @param transactFamily
   * @param privateKey - Private key of this client (may be different from the process that will
   * generate the bathes)
   * @throws NoSuchAlgorithmException
   */
  public TransactionFactory(TransactionFamily transactFamily, ECKey privateKey)
      throws NoSuchAlgorithmException {
    super(privateKey);
    if (transactFamily == null) {
      throw new InvalidParameterException("Null Transaction Family");
    }
    this.messagesTFamily = transactFamily;

    FACTORY_MESSAGEDIGESTER.set(MessageDigest.getInstance(transactFamily.getDigesterAlgorythm()));

    LOGGER.trace("Created Message Factory for Transation Family {}, version {}.",
        transactFamily.getFamilyName(), transactFamily.getFamilyVersion());

  }

  private String createHeaderSignature(TransactionHeader header) {
    return SawtoothSigner.signHexSequence(this.privateKey, header.toByteArray());
  }

  public Transaction createTransaction(ByteBuffer payload, List<String> inputs,
      List<String> outputs, List<String> dependencies, String signerPubKey)
      throws NoSuchAlgorithmException, InvalidProtocolBufferException {

    Transaction.Builder transactionBuilder = Transaction.newBuilder();
    ByteString payloadToTransaction = ByteString.copyFrom(payload.array());
    transactionBuilder.setPayload(payloadToTransaction);
    TransactionHeader header;
    if (signerPubKey == null || signerPubKey.isEmpty()) {
      LOGGER.info("No public key informed, using the on from the factory...");
      LOGGER.trace("Creating transaction for family {} with internal public key {}... ",
          this.messagesTFamily.getFamilyName(), privateKey.getPublicKeyAsHex());
      header = createTransactionHeader(generateDigestHex(payloadToTransaction.toByteArray()),
          inputs, outputs, dependencies, Boolean.TRUE, privateKey.getPublicKeyAsHex());
    } else {
      LOGGER.trace("Creating transaction for family {} with batcher public key {}... ",
          this.messagesTFamily.getFamilyName(), signerPubKey);
      header = createTransactionHeader(generateDigestHex(payloadToTransaction.toByteArray()),
          inputs, outputs, dependencies, Boolean.TRUE, signerPubKey);
    }
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
    String hexFormattedDigest = generateDigestHex(theRequest.getPayload().toByteArray());
    TransactionHeader header = createTransactionHeader(hexFormattedDigest,
        theRequest.getHeader().getInputsList(), theRequest.getHeader().getOutputsList(),
        theRequest.getHeader().getDependenciesList(), Boolean.TRUE,
        theRequest.getHeader().getBatcherPublicKey());
    transactionBuilder.setHeader(header.toByteString());
    transactionBuilder.setHeaderSignature(createHeaderSignature(header));

    return transactionBuilder.build();
  }

  private final TransactionHeader createTransactionHeader(String payloadSha512, List<String> inputs,
      List<String> outputs, List<String> dependencies, boolean needsNonce, String batcherPubKey)
      throws InvalidProtocolBufferException {
    if (batcherPubKey == null || batcherPubKey.isEmpty()) {
      throw new InvalidProtocolBufferException("No batcher public key informed.");
    }
    TransactionHeader.Builder thBuilder = TransactionHeader.newBuilder();
    thBuilder.setFamilyName(this.messagesTFamily.getFamilyName());
    thBuilder.setFamilyVersion(this.messagesTFamily.getFamilyVersion());
    thBuilder.setSignerPublicKey(this.privateKey.getPublicKeyAsHex());
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
