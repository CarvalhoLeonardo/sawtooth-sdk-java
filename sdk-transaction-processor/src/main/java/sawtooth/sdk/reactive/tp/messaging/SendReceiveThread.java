// @formatter:off
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

package sawtooth.sdk.reactive.tp.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import com.google.protobuf.InvalidProtocolBufferException;
import sawtooth.sdk.protobuf.Message;

/**
 * An internal messaging implementation used by the DefaultMessageStreamImpl class.
 */
class SendReceiveThread implements Runnable {

  private String url;
  private ZMQ.Socket socket;
  private Lock lock = new ReentrantLock();
  private Condition condition = lock.newCondition();
  private ConcurrentHashMap<String, Future<Message>> futures;
  private LinkedBlockingQueue<MessageWrapper> receiveQueue;
  private ZContext context;
  private final static Logger LOGGER = LoggerFactory.getLogger(SendReceiveThread.class);

  public SendReceiveThread(String url, ConcurrentHashMap<String, Future<Message>> futures,
      LinkedBlockingQueue<MessageWrapper> recvQueue) {
    super();
    this.url = url;
    this.futures = futures;
    this.receiveQueue = recvQueue;
    this.context = null;
  }

  /**
   * Inner class for passing messages.
   */
  public class MessageWrapper {
    Message message;

    public MessageWrapper(Message message) {
      this.message = message;
    }
  }

  private class DisconnectThread extends Thread {
    protected LinkedBlockingQueue<MessageWrapper> receiveQueue;
    protected ConcurrentHashMap<String, Future<Message>> futures;

    public DisconnectThread(LinkedBlockingQueue<MessageWrapper> receiveQueue,
        ConcurrentHashMap<String, Future<Message>> futures) {
      this.receiveQueue = receiveQueue;
      this.futures = futures;
    }
  }

  /**
   * Inner class for receiving messages.
   */
  private class Receiver implements ZLoop.IZLoopHandler {

    private ConcurrentHashMap<String, Future<Message>> futures;
    private LinkedBlockingQueue<MessageWrapper> receiveQueue;

    Receiver(ConcurrentHashMap<String, Future<Message>> futures,
        LinkedBlockingQueue<MessageWrapper> receiveQueue) {
      this.futures = futures;
      this.receiveQueue = receiveQueue;
    }

    @Override
    public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
      LOGGER.debug("-- handle() --");
      ZMsg msg = ZMsg.recvMsg(item.getSocket());
      Iterator<ZFrame> multiPartMessage = msg.iterator();

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      while (multiPartMessage.hasNext()) {
        ZFrame frame = multiPartMessage.next();
        try {
          byteArrayOutputStream.write(frame.getData());
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
      LOGGER.debug("-- message :  --"+msg.toString());
      try {
        Message message = Message.parseFrom(byteArrayOutputStream.toByteArray());
        if (this.futures.containsKey(message.getCorrelationId())) {
          LOGGER.debug("-- sawtooth message queued for future "+message.getCorrelationId()+" :  --"+message.toString());
          this.futures.put(message.getCorrelationId(), CompletableFuture.completedFuture(message));
        }
        MessageWrapper wrapper = new MessageWrapper(message);
        LOGGER.debug("-- sawtooth message queued :  --"+message.toString());
        this.receiveQueue.put(wrapper);

      } catch (InterruptedException ie) {
        ie.printStackTrace();
      } catch (InvalidProtocolBufferException ipe) {
        ipe.printStackTrace();
      }

      return 0;
    }
  }

  @Override
  public void run() {
    this.context = new ZContext();
    socket = this.context.createSocket(ZMQ.DEALER);
    socket.monitor("inproc://monitor.s", ZMQ.EVENT_DISCONNECTED);
    final ZMQ.Socket monitor = this.context.createSocket(ZMQ.PAIR);
    monitor.connect("inproc://monitor.s");
    new DisconnectThread(this.receiveQueue, this.futures) {
      @Override
      public void run() {
        while (true) {
          // blocks until disconnect event recieved
          ZMQ.Event event = ZMQ.Event.recv(monitor);
          if (event.getEvent() == ZMQ.EVENT_DISCONNECTED) {
            try {
              MessageWrapper disconnectMsg = new MessageWrapper(null);
              for (String key : this.futures.keySet()) {
                Future<Message> toChange = this.futures.get(key);
                toChange.cancel(true);
                this.futures.put(key, toChange);
              }
              this.receiveQueue.clear();
              this.receiveQueue.put(disconnectMsg);
            } catch (InterruptedException ie) {
              ie.printStackTrace();
            }
          }
        }
      }
    }.start();

    socket.setIdentity((this.getClass().getName() + UUID.randomUUID().toString()).getBytes());
    socket.connect(url);
    lock.lock();
    try {
      condition.signalAll();
    } finally {
      lock.unlock();
    }
    ZLoop eventLoop = new ZLoop(this.context);
    ZMQ.PollItem pollItem = new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN);
    eventLoop.addPoller(pollItem, new Receiver(futures, receiveQueue), new Object());
    eventLoop.start();
  }

  /**
   * Used by the Stream class to send a message.
   * 
   * @param message protobuf Message
   */
  public Future<Message> sendMessage(Message message) {
    lock.lock();
    try {
      if (socket == null) {
        condition.await();
      }
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } finally {
      lock.unlock();
    }
    ZMsg msg = new ZMsg();
    msg.add(message.toByteString().toByteArray());
    msg.send(socket);
    return CompletableFuture.completedFuture(message);
  }

  /**
   * Ends the zmq communication.
   */
  public void stop() {
    this.socket.close();
    this.context.destroy();
  }

}
