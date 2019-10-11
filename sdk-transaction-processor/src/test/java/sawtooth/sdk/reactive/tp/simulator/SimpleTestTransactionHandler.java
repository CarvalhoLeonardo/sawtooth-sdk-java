package sawtooth.sdk.reactive.tp.simulator;

import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.protobuf.TpProcessResponse.Status;
import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.common.message.factory.BatchFactory;
import sawtooth.sdk.reactive.common.message.factory.TransactionFactory;
import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;
import sawtooth.sdk.reactive.tp.message.factory.FamilyRegistryMessageFactory;
import sawtooth.sdk.reactive.tp.processor.SawtoothState;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;

/**
 *
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * This implementation is intended to answer simple requests, like PING and REGISTRATION_REQUEST
 *
 */
public class SimpleTestTransactionHandler implements TransactionHandler {

  private final static Logger LOGGER = LoggerFactory.getLogger(SimpleTestTransactionHandler.class);
  private static ECKey pubKey = null;
  private final static ECKey randomPrivateKey = new ECKey();
  private final static BatchFactory TEST_BATCH_FACTORY;
  private final static CoreMessagesFactory TEST_CORE_MESSAGE_FACTORY;
  private final static TransactionFamily TEST_MESSAGE_FAMILY;
  private final static FamilyRegistryMessageFactory TEST_REGISTRY_MESSAGE_FACTORY;
  private final static TransactionFactory TEST_TRANSACTION_FACTORY;

  static {
    TransactionFamily tmpMF = new TransactionFamily("coretests", "0.0",
        new String[] { "coretest" });
    CoreMessagesFactory tmpCMF = null;
    FamilyRegistryMessageFactory tmpRMF = null;
    TransactionFactory tmpTFF = null;
    BatchFactory tmpBFF = null;
    pubKey = ECKey.fromPublicOnly(randomPrivateKey.getPubKeyPoint());
    LOGGER.debug("Created EC Private {} and Public {}", randomPrivateKey.getPrivateKeyAsHex(),
        pubKey.getPublicKeyAsHex());
    try {
      tmpCMF = new CoreMessagesFactory();
      tmpRMF = new FamilyRegistryMessageFactory(randomPrivateKey, tmpMF);
      tmpTFF = new TransactionFactory(tmpMF, randomPrivateKey);
      tmpBFF = new BatchFactory(randomPrivateKey);

    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    TEST_MESSAGE_FAMILY = tmpMF;
    TEST_CORE_MESSAGE_FACTORY = tmpCMF;
    TEST_REGISTRY_MESSAGE_FACTORY = tmpRMF;
    TEST_TRANSACTION_FACTORY = tmpTFF;
    TEST_BATCH_FACTORY = tmpBFF;
  }
  private byte[] externalContextID = null;

  private String internalErrorSignature = "";
  private String invalidTransactionSignature = "";

  @Override
  public CompletableFuture<TpProcessResponse> apply(TpProcessRequest processRequest,
      SawtoothState state) {
    TpProcessResponse.Builder responseBuilder = TpProcessResponse.newBuilder();
    if (processRequest.getSignature().equalsIgnoreCase(internalErrorSignature)) {
      LOGGER.error("Signature {} triggering Internal error! ", internalErrorSignature);
      responseBuilder.setStatus(Status.INTERNAL_ERROR);
      responseBuilder.setMessage("INTERNAL ERROR");
    } else if (processRequest.getSignature().equalsIgnoreCase(invalidTransactionSignature)) {
      LOGGER.error("Signature {} triggering Invalid Transaction ! ", invalidTransactionSignature);
      responseBuilder.setStatus(Status.INVALID_TRANSACTION);
      responseBuilder.setMessage("INVALID TRANSACTION");
    } else {
      responseBuilder.setStatus(Status.STATUS_UNSET);
      responseBuilder.setMessage("Not Yet Implemented");
    }
    return CompletableFuture.completedFuture(responseBuilder.build());
  }

  @Override
  public BatchFactory getBatchFactory() {
    return TEST_BATCH_FACTORY;
  }

  @Override
  public CoreMessagesFactory getCoreMessageFactory() {
    return TEST_CORE_MESSAGE_FACTORY;
  }

  public final byte[] getExternalContextID() {
    return externalContextID;
  }

  @Override
  public FamilyRegistryMessageFactory getFamilyRegistryMessageFactory() {
    return TEST_REGISTRY_MESSAGE_FACTORY;
  }

  @Override
  public Collection<String> getNameSpaces() {
    return TEST_MESSAGE_FAMILY.getNameSpaces().keySet();
  }

  @Override
  public TransactionFactory getTransactionFactory() {
    return TEST_TRANSACTION_FACTORY;
  }

  @Override
  public TransactionFamily getTransactionFamily() {
    return TEST_MESSAGE_FAMILY;
  }

  @Override
  public ECKey getTransactorPubKey() {
    return pubKey;
  }

  @Override
  public void setContextId(byte[] externalContextID) {
    this.externalContextID = externalContextID;
  }

  /**
   * To test failure behavior, prepare a specific signature to trigger a Status.INTERNAL_ERROR
   *
   * @param corrId
   */
  public void setSignatureInternalError(String failingSig) {
    this.internalErrorSignature = failingSig;
  }

  /**
   * To test failure behavior, prepare a specific signature to trigger a Status.INVALID_TRANSACTION.
   *
   * @param corrId
   */
  public void setSignatureInvalidTransaction(String failingSig) {
    this.invalidTransactionSignature = failingSig;
  }
}
