package sawtooth.sdk.reactive.tp.fake;

import java.lang.reflect.InaccessibleObjectException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.tp.processor.SawtoothState;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;

/**
 * 
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         This implementation is intended to answer simple requests, like PING and
 *         REGISTRATION_REQUEST
 *
 */
public class SimpleTestTransactionHandler implements TransactionHandler {

  private final static Logger LOGGER = LoggerFactory.getLogger(SimpleTestTransactionHandler.class);
  public final static MessageFactory TEST_MESSAGE_FACTORY;
  static {
    MessageFactory tmpMF = null;
    try {
      tmpMF = new MessageFactory("ping", "0.0.0", null, null, "ping");
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    TEST_MESSAGE_FACTORY = tmpMF;
  }


  @Override
  public String transactionFamilyName() {
    return TEST_MESSAGE_FACTORY.getFamilyName();
  }

  @Override
  public String getVersion() {
    return TEST_MESSAGE_FACTORY.getFamilyVersion();
  }

  @Override
  public Collection<String> getNameSpaces() {
    return Arrays.asList(TEST_MESSAGE_FACTORY.getNameSpaces());
  }

  @Override
  public MessageFactory getMessageFactory() {
    return TEST_MESSAGE_FACTORY;
  }

  @Override
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state){
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setMessageFactory(MessageFactory mFactory) {
    throw new InaccessibleObjectException();
    
  }

 
}
