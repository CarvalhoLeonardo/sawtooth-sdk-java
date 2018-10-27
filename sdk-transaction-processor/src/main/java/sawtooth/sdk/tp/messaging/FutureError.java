// @formatter:off
/*------------------------------------------------------------------------------

 Copyright 2017 Intel Corporation
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
package sawtooth.sdk.tp.messaging;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;

import sawtooth.sdk.common.exceptions.ValidatorConnectionError;
import sawtooth.sdk.protobuf.Message;

/**
 * FutureError throws a ValidatorConnectionError from all of its methods. Used to resolve a future
 * with an error response.
 */
public class FutureError implements Future<Message> {

  /**
   * Constructor.
   */
  public FutureError() {
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Message get() throws InterruptedException, ExecutionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Message get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Always raises ValidatorConnectionError.
   * @throws ValidatorConnectionError Always throws this Exception.
   * @return Does not return.
   */
  public final ByteString getResult() throws ValidatorConnectionError {
    throw new ValidatorConnectionError();
  }

  /**
   * Always raises ValidatorConnectionError.
   * @param time The timeout.
   * @throws ValidatorConnectionError Always throws this Exception.
   * @throws TimeoutException Does not throw this exception.
   * @return Does not return.
   */
  public final ByteString getResult(final long time)
      throws TimeoutException, ValidatorConnectionError {
    throw new ValidatorConnectionError();
  }

  @Override
  public boolean isCancelled() {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * @return Always return false
   */
  @Override
  public final boolean isDone() {
    return Boolean.FALSE;
  }

  /**
   * Always raises ValidatorConnectionError.
   * @param byteString The bytes.
   * @throws ValidatorConnectionError Always throws this exception.
   */
  public final void setResult(final ByteString byteString) throws ValidatorConnectionError {
    throw new ValidatorConnectionError();
  }

}
