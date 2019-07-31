package sawtooth.sdk.reactive.common.messaging;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 * @author Leonardo T. de Carvalho
 * 
 *         <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 *         <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 * 
 *         Since each Message Factory follows a particular Adress schema, this interface defines a
 *         common API to do so.
 *
 */
public interface SawtoothAddressFactory {

  public String generateAddress(String nameSpace, String address);
  
  public List<String> generateAddresses(String nameSpace, String... addresses);
  
  public String generateAddress(String nameSpace, ByteBuffer data);
  
  
}
