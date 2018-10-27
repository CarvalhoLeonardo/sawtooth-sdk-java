package sawtooth.sdk.reactive.tp.fake;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InaccessibleObjectException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TpProcessResponse;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.common.messaging.SawtoothAddressFactory;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;
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
 *         Don't forget to run "sawset proposal create
 *         sawtooth.validator.transaction_families='[..., {"family":"sawtooth_settings",
 *         "version":"1.0"}, {"family":"coretests", "version":"0.0"}]'"
 *
 */
public class SimpleTestTransactionHandler implements TransactionHandler, SawtoothAddressFactory {

  private final static Logger LOGGER = LoggerFactory.getLogger(SimpleTestTransactionHandler.class);

  public final static MessageFactory TEST_MESSAGE_FACTORY;

  static {
    MessageFactory tmpMF = null;
    try {
      tmpMF = new MessageFactory("coretests", "0.0", null, null, "coretest");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    TEST_MESSAGE_FACTORY = tmpMF;
  }
  private byte[] externalContextID = null;



  @Override
  public CompletableFuture<TpProcessResponse> executeProcessRequest(TpProcessRequest processRequest,
      SawtoothState state) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public final String generateAddress(final String nSpace, final ByteBuffer data) {
    String hData = FormattingUtils.hash512(data.array());
    return TEST_MESSAGE_FACTORY.getNameSpaces().get(nSpace)
        + hData.substring(hData.length() - MESSAGE_SIZE_DELIMITER);
  }


  @Override
  public final String generateAddress(final String nSpace, final String address) {
    String hashedName = "";
    try {
      hashedName = FormattingUtils.hash512(address.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return TEST_MESSAGE_FACTORY.getNameSpaces().get(nSpace)
        + hashedName.substring(hashedName.length() - MESSAGE_SIZE_DELIMITER);
  }

  @Override
  public List<String> generateAddresses(String nameSpace, String... addresses) {
    return Arrays.asList(addresses).stream().map(es -> {
      return generateAddress(nameSpace, es);
    }).collect(Collectors.toList());
  }

  public final byte[] getExternalContextID() {
    return externalContextID;
  }

  @Override
  public MessageFactory getMessageFactory() {
    return TEST_MESSAGE_FACTORY;
  }

  @Override
  public Collection<String> getNameSpaces() {
    return TEST_MESSAGE_FACTORY.getNameSpaces().keySet();
  }

  @Override
  public String getVersion() {
    return TEST_MESSAGE_FACTORY.getFamilyVersion();
  }

  @Override
  public void setContextId(byte[] externalContextID) {
    this.externalContextID = externalContextID;
  }

  @Override
  public void setMessageFactory(MessageFactory mFactory) {
    throw new InaccessibleObjectException();

  }

  @Override
  public String transactionFamilyName() {
    return TEST_MESSAGE_FACTORY.getFamilyName();
  }


}
