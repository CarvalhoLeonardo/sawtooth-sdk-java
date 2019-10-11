package sawtooth.sdk.reactive.tp.fake.factory.message;

import java.security.NoSuchAlgorithmException;

import sawtooth.sdk.reactive.tp.message.factory.CoreMessagesFactory;

public class FakeValidatorCoreMF extends CoreMessagesFactory {

  public FakeValidatorCoreMF() throws NoSuchAlgorithmException {
    super();
  }

  public FakeValidatorCoreMF(String digesterAlgo) throws NoSuchAlgorithmException {
    super(digesterAlgo);
  }

}
