package sawtooth.sdk.reactive.tp.tests;

import java.security.NoSuchAlgorithmException;
import sawtooth.sdk.reactive.common.messaging.MessageFactory;
import sawtooth.sdk.reactive.tp.stress.StressValidator;

public class StartStress {


  public static void main(String[] args) throws NoSuchAlgorithmException {

    MessageFactory intTP = new MessageFactory("intkey", "1.0", null, null, "intkey");
    StressValidator myValidator = new StressValidator(intTP, "ipc://lalalala");
    Thread thread = new Thread(myValidator);
    thread.start();
  }

}
