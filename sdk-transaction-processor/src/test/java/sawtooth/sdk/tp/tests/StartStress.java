package sawtooth.sdk.tp.tests;

import java.security.NoSuchAlgorithmException;

import sawtooth.sdk.common.messaging.MessageFactory;
import sawtooth.sdk.tp.stress.StressValidator;

public class StartStress {

  public static void main(String[] args) throws NoSuchAlgorithmException {

    MessageFactory intTP = new MessageFactory("intkey", "1.0", null, null, "intkey");
    StressValidator myValidator = new StressValidator(intTP, "ipc://lalalala");
    Thread thread = new Thread(myValidator);
    thread.start();
  }

}
