package sawtooth.examples.intkey;

import java.util.Arrays;
import java.util.List;

public enum TPOperations {

  /**
   * Enumeration of the possible operations.
   */
  DEC("dec", "Decrement the value at the address"), INC("inc",
      "Increment the value at the address"), SET("set", "Set a value at a not set address");

  /**
   * List of Operations.
   *
   * @return Operations
   */
  public static List<TPOperations> getAllOperations() {
    return Arrays.asList(values());
  }

  /**
   * Ger an Operation by its verb.
   *
   * @param verb
   * @return The Operation
   */
  public static TPOperations getByVerb(final String verb) {
    for (TPOperations eachOne : values()) {
      if (eachOne.getVerb().equalsIgnoreCase(verb)) {
        return eachOne;
      }
    }

    return null;
  }

  /**
   * Description of the verb.
   */
  private final String description;

  /**
   * The verb.
   */
  private final String verb;

  /**
   * Definition.
   *
   * @param verb
   * @param description
   */
  TPOperations(final String verb, final String description) {
    this.verb = verb;
    this.description = description;
  }

  /**
   *
   * Getter for description.
   *
   * @return description.
   */
  public final String getDescription() {
    return description;
  }

  /**
   *
   * Getter for verb.
   *
   * @return verb.
   */

  public final String getVerb() {
    return verb;
  }
}
