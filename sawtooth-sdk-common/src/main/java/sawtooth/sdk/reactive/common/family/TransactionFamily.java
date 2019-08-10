package sawtooth.sdk.reactive.common.family;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sawtooth.sdk.reactive.common.message.factory.SawtoothAddressFactory;
import sawtooth.sdk.reactive.common.utils.FormattingUtils;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * Class that defines the Transaction Family, and encapsulates the Address Generation for it.
 *
 * The boundaries are detailed in
 * https://sawtooth.hyperledger.org/docs/core/releases/1.1/app_developers_guide/address_and_namespace.html
 *
 */

public class TransactionFamily implements SawtoothAddressFactory {

  private final static int ADDRESS_PREFIX_SIZE = 6;
  private final static int ADDRESS_SIZE = 70;
  private final static int ADDRESS_SUFFIX_SIZE = ADDRESS_SIZE - ADDRESS_PREFIX_SIZE;
  private final static String DEFAULT_DIGESTER_ALGO = "SHA-512";
  final String digesterAlgo;
  final String familyName;
  final String familyVersion;
  final Map<String, String> nameSpacesMap;

  @SuppressWarnings("unused")
  private TransactionFamily() {
    nameSpacesMap = null;
    familyName = null;
    familyVersion = null;
    digesterAlgo = DEFAULT_DIGESTER_ALGO;
  }

  public TransactionFamily(String familyName, String familyVersion, String... nameSpaces) {
    this(familyName, familyVersion, DEFAULT_DIGESTER_ALGO, nameSpaces);
  }

  public TransactionFamily(String familyName, String familyVersion, String digesterAlgo,
      String... nameSpaces) {
    super();
    this.digesterAlgo = digesterAlgo;
    this.familyName = familyName;
    this.familyVersion = familyVersion;
    this.nameSpacesMap = new HashMap<String, String>();
    for (String eachNS : nameSpaces) {
      nameSpacesMap.put(eachNS, FormattingUtils.hash512(eachNS.getBytes(StandardCharsets.UTF_8))
          .substring(0, ADDRESS_PREFIX_SIZE));
    }
  }

  @Override
  public String generateAddress(String nameSpace, String address) {
    return nameSpacesMap.get(nameSpace) + FormattingUtils
        .hash512(address.getBytes(StandardCharsets.UTF_8)).substring(ADDRESS_PREFIX_SIZE);
  }

  @Override
  public List<String> generateAddresses(String nameSpace, String... addresses) {
    return Stream.of(addresses).map(es -> {
      return generateAddress(nameSpace, es);
    }).collect(Collectors.toList());

  }

  public final String getDigesterAlgorythm() {
    return digesterAlgo;
  }

  public final String getFamilyName() {
    return familyName;
  }

  public final String getFamilyVersion() {
    return familyVersion;
  }

  public final Map<String, String> getNameSpaces() {
    return nameSpacesMap;
  }

}
