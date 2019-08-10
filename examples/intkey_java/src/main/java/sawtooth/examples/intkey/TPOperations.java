package sawtooth.examples.intkey;

import java.util.Arrays;
import java.util.List;

public enum TPOperations {

  INC("inc","Increment the value at the address"),
  DEC("dec","Decrement the value at the address"),
  SET("set","Set a value at a not set address");

  public final String getVerb() {
    return verb;
  }

  public final String getDescription() {
    return description;
  }

  private final String verb;
  private final String description;

  private TPOperations(String verb, String description) {
    this.verb=verb;
    this.description = description;
  }

  public static List<TPOperations> getAllOperations(){
    return Arrays.asList(values());
  }

  public static TPOperations getByVerb(String verb) {
    for (TPOperations eachOne : values()) {
      if (eachOne.getVerb().equalsIgnoreCase(verb)) {
        return eachOne;
      }
    }

    return null;
  }
}
