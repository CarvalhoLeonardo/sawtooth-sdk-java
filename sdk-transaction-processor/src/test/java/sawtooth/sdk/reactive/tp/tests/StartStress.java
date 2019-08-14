package sawtooth.sdk.reactive.tp.tests;

import java.security.NoSuchAlgorithmException;

import sawtooth.sdk.reactive.common.family.TransactionFamily;
import sawtooth.sdk.reactive.tp.stress.StressValidator;

public class StartStress {

  public static void main(String[] args) throws NoSuchAlgorithmException {

    TransactionFamily intTP = new TransactionFamily("intkey", "1.0", null,
        new String[] { "intkey" });
    StressValidator myValidator = new StressValidator(intTP, "ipc://lalalala");
    Thread thread = new Thread(myValidator);
    thread.start();
  }

}
