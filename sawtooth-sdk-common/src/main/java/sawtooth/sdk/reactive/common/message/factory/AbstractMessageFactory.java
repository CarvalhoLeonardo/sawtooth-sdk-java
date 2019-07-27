package sawtooth.sdk.reactive.common.message.factory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.bitcoinj.core.ECKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import com.google.protobuf.GeneratedMessageV3;

import sawtooth.sdk.reactive.common.crypto.SawtoothSigner;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;

public abstract class AbstractMessageFactory<T extends GeneratedMessageV3> {

  protected static Logger LOGGER = LoggerFactory.getLogger(AbstractMessageFactory.class);
  final String digesterAlgo;

  final String familyName;
  final String familyVersion;
  final Marker logMarker;
  final Map<String, String> nameSpacesMap;

  ECKey privateKey;

  @SuppressWarnings("unused")
  private AbstractMessageFactory() throws NoSuchAlgorithmException {

    privateKey = null;
    nameSpacesMap = null;
    familyName = null;
    familyVersion = null;
    digesterAlgo = null;
    logMarker = null;
  }

  public AbstractMessageFactory(String familyName, String familyVersion, ECKey privateKey,
      String... nameSpaces) throws NoSuchAlgorithmException {
    this(familyName, "SHA-512", familyVersion, privateKey, nameSpaces);
  }

  public AbstractMessageFactory(String familyName, String digesterAlgo, String familyVersion,
      ECKey privateKey, String... nameSpaces) throws NoSuchAlgorithmException {
    logMarker = MarkerFactory.getMarker(this.getClass().getName());
    this.digesterAlgo = digesterAlgo;
    this.familyName = familyName;
    this.familyVersion = familyVersion;
    if (privateKey == null) {
      LOGGER.warn(logMarker, "Private Key null, creating a temporary one...");
      this.privateKey = SawtoothSigner.generatePrivateKey(new SecureRandom(ByteBuffer
          .allocate(Long.BYTES).putLong(Calendar.getInstance().getTimeInMillis()).array()));
      LOGGER.warn("Created with encryption " + this.privateKey.getEncryptionType().toString()
          + " and Key Crypter " + this.privateKey.getKeyCrypter());
    } else {
      this.privateKey = privateKey;
    }

    LOGGER.debug("Public key {}.", privateKey.getPubKey());

    nameSpacesMap = new HashMap<String, String>();
    for (String eachNS : nameSpaces) {
      nameSpacesMap.put(eachNS,
          FormattingUtils.hash512(eachNS.getBytes(StandardCharsets.UTF_8)).substring(0, 6));
    }
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  public abstract T create();

}
