//@formatter:off
/*-----------------------------------------------------------------------------
 Copyright 2016, 2017 Intel Corporation

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
// @formatter:on

package sawtooth.sdk.reactive.tp.processor;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface TransactionProcessor {

  /**
   * Add a new handler that will process the messages adressed to it.
   *
   * @param handler implements the TransactionHandler interface
   */
  public void addHandler(TransactionHandler handler);

  /**
   * Disables a handler.
   *
   * @param transactionFamilyName - Family of the handler
   * @param version - Version of the Handler
   */
  public void disableHandler(String transactionFamilyName, String version);

  public byte[] getExternalContextID();

  public String getTransactionProcessorId();

  public void init() throws InterruptedException, ExecutionException;

  public List<TransactionHandler> listRegisteredHandlers();

  public void shutdown();


}
