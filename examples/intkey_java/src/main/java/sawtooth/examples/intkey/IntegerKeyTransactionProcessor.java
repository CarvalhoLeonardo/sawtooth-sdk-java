/* Copyright 2016 Intel Corporation
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
------------------------------------------------------------------------------*/

package sawtooth.examples.intkey;

import java.util.concurrent.ExecutionException;
import sawtooth.examples.intkey.IntegerKeyHandler;
import sawtooth.sdk.reactive.tp.processor.DefaultTransactionProcessorImpl;
import sawtooth.sdk.reactive.tp.processor.TransactionProcessor;

public class IntegerKeyTransactionProcessor {

  /**
   * the method that runs a Thread with a TransactionProcessor in it.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException, ExecutionException {

    TransactionProcessor transactionProcessor = new DefaultTransactionProcessorImpl(args[0],args[1], Integer.parseInt(args[2]));
    transactionProcessor.init();
    transactionProcessor.addHandler(new IntegerKeyHandler());

  }
}
