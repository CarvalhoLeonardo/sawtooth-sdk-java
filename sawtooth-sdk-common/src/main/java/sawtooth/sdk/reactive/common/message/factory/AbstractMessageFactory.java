package sawtooth.sdk.reactive.common.message.factory;

import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.bitcoin.NativeSecp256k1;
import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessageV3;

import sawtooth.sdk.reactive.common.utils.FormattingUtils;

public abstract class AbstractMessageFactory<T extends GeneratedMessageV3> {

  protected static Logger LOGGER = LoggerFactory.getLogger(AbstractMessageFactory.class);
  protected ThreadLocal<MessageDigest> FACTORY_MESSAGEDIGESTER = new ThreadLocal<>();
  protected ECKey privateKey;

  protected AbstractMessageFactory() throws NoSuchAlgorithmException {
    privateKey = null;
  }

  public AbstractMessageFactory(ECKey privateKey) throws NoSuchAlgorithmException {
    if (privateKey == null) {
      throw new InvalidParameterException("Null Private Key");
    }
    if (!NativeSecp256k1.secKeyVerify(privateKey.getPrivKeyBytes())) {
      throw new InvalidParameterException(
          "Invalid NativeSecp256k1 Private Key " + privateKey.toASN1());
    }
    this.privateKey = privateKey;

    LOGGER.trace("Private key {}.", privateKey);
    LOGGER.trace("Public key {}.", privateKey.getPubKey());
  }

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
