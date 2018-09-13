package sawtooth.examples.intkey;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 *         Definitions of the operations domains.
 *
 */
public enum TPOperations {

  /**
   * DEC
   */
  DEC("dec", "Decrement the value at the address"),


  /**
   * INC
   */
  INC("inc", "Increment the value at the address"),


  /**
   * SET
   */
  SET("set", "Set a value at a not set address");

  public static List<TPOperations> getAllOperations() {
    return Arrays.asList(values());
  }

  public static TPOperations getByVerb(final String verb) {
    for (TPOperations eachOne : values()) {
      if (eachOne.getVerb().equalsIgnoreCase(verb)) {
        return eachOne;
      }
    }

    return null;
  }

  private final String description;
  private final String verb;

  private TPOperations(final String verb, final String description) {
    this.verb = verb;
    this.description = description;
  }

  public final String getDescription() {
    return description;
  }

  public final String getVerb() {
    return verb;
  }
}
