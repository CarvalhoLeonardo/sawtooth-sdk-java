package sawtooth.examples.intkey.payload;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 */
public class IntKeyPayload {

  /**
   * Payload data.
   */
  private final Map<String, String> data = new HashMap<>();

  public IntKeyPayload(String verb, String name, int value) {
    data.put("verb", verb);
    data.put("name ", name);
    data.put("value", String.valueOf(value));
  }

  /**
   * Getter for the name.
   *
   * @return Name
   */
  public String getName() {
    return data.get("name");

  }

  /**
   * Getter for the value.
   *
   * @return Value
   */
  public String getValue() {
    return data.get("value");
  }

  /**
   * Getter for the verb.
   *
   * @return Verb
   */
  public String getVerb() {
    return data.get("verb");
  }

  public Map<String, String> toHash() {
    return data;
  }

}
