package sawtooth.sdk.reactive.tp.fake;

import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
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
 * Don't forget to run "sawset proposal create sawtooth.validator.transaction_families='[...,
 * {"family":"sawtooth_settings", "version":"1.0"}, {"family":"coretests", "version":"0.0"}]'"
 *
 */
public class SimpleTestTransactionHandler implements TransactionHandler {

  private final static Logger LOGGER = LoggerFactory.getLogger(SimpleTestTransactionHandler.class);
  private final static ECKey randomPrivateKey = new ECKey();
  private final static CoreMessagesFactory TEST_CORE_MESSAGE_FACTORY;
  private final static TransactionFamily TEST_MESSAGE_FAMILY;
  private final static FamilyRegistryMessageFactory TEST_REGISTRY_MESSAGE_FACTORY;

  static {
    TransactionFamily tmpMF = new TransactionFamily("coretests", "0.0",
        new String[] { "coretest" });
    CoreMessagesFactory tmpCMF = null;
    FamilyRegistryMessageFactory tmpRMF = null;
    try {
      tmpCMF = new CoreMessagesFactory();
      tmpRMF = new FamilyRegistryMessageFactory(randomPrivateKey, tmpMF);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    TEST_MESSAGE_FAMILY = tmpMF;
    TEST_CORE_MESSAGE_FACTORY = tmpCMF;
    TEST_REGISTRY_MESSAGE_FACTORY = tmpRMF;
  }
  private byte[] externalContextID = null;

  @Override
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BatchFactory getBatchFactory() {
    // TODO Auto-generated method stub
    return null;
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
    return TEST_MESSAGE_FAMILY.getNameSpaces().values();
  }

  @Override
  public TransactionFactory getTransactionFactory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TransactionFamily getTransactionFamily() {
    return TEST_MESSAGE_FAMILY;
  }

  @Override
  public void setContextId(byte[] externalContextID) {
    this.externalContextID = externalContextID;
  }

}
