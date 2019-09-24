package sawtooth.sdk.reactive.common.message.factory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.protobuf.GeneratedMessageV3;

import sawtooth.sdk.reactive.common.utils.FormattingUtils;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * @param <T> Simple Sawtooth message to be generated. This messages are not related to
 * transactions, but used in othr processes
 */
public abstract class AbstractMessageFactory<T extends GeneratedMessageV3> {

  private static final ThreadLocal<MessageDigest> FACTORY_MESSAGEDIGESTER = ThreadLocal
      .withInitial(new Supplier<MessageDigest>() {
        @Override
        public MessageDigest get() {
          try {
            return MessageDigest.getInstance("SHA-512");
          } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
          }
          return null;
        }
      });

  @Override
  protected Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  protected String generateDigestHex(byte[] toHash) {
    FACTORY_MESSAGEDIGESTER.get().reset();
    FACTORY_MESSAGEDIGESTER.get().update(toHash, 0, toHash.length);
    return FormattingUtils.bytesToHex(FACTORY_MESSAGEDIGESTER.get().digest());
  }

  /**
   * generate a random String using the sha-512 algorithm, to correlate sent messages with futures
   *
   * Being random, we dont need to reset() the digester
   *
   * @return a random String
   */
  protected String generateId() {
    return FormattingUtils
        .bytesToHex(FACTORY_MESSAGEDIGESTER.get().digest(UUID.randomUUID().toString().getBytes()))
        .substring(0, 22);
  }

}
