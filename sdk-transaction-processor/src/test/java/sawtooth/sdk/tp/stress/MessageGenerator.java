package sawtooth.sdk.tp.stress;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.util.concurrent.Queues;
import sawtooth.sdk.common.messaging.MessageFactory;
import sawtooth.sdk.protobuf.Message;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * The class will generate messages, a LOT of them.
 *
 */
public class MessageGenerator {

  private class RunFluxForFactory implements Runnable {
    private final MessageFactory mf;

    @SuppressWarnings("unused")
    private RunFluxForFactory() {
      mf = null;
    }

    public RunFluxForFactory(MessageFactory mf) {
      super();
      this.mf = mf;
    }

    @Override
    public void run() {
      Flux.<Message>create(messageSink -> {
        MessageFactory loopmf = this.mf;
        Message pingMessage = null;
        LOGGER.debug("messagesFlux for " + mf.getFamilyName() + " : starting loop...");
        while (!Thread.currentThread().isInterrupted()) {
          try {
            pingMessage = loopmf.getPingRequest(null);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
          messageSink.next(pingMessage);
        }
        LOGGER.debug("messagesFlux for " + mf.getFamilyName() + " : interrupted");
        messageSink.complete();
      }).subscribeWith(senderProcessor);

    }

  }

  private static final List<Runnable> agents = new ArrayList<>();
  private final static Logger LOGGER = LoggerFactory.getLogger(MessageGenerator.class);

  public static ArrayList<Integer> messagesSizes = new ArrayList<>();

  static Random rand = new Random(System.nanoTime());
  private static WorkQueueProcessor<Message> senderProcessor;

  ExecutorService localExecutor;

  @SuppressWarnings("unused")
  private MessageGenerator() {

  }

  public MessageGenerator(int threadCount) {
    localExecutor = Executors.newWorkStealingPool(threadCount + 2);

    senderProcessor = WorkQueueProcessor.<Message>builder().bufferSize(Queues.SMALL_BUFFER_SIZE)
        .executor(localExecutor).build();

  }

  public final Flux<Message> getMessagesflux(Duration time) {
    return senderProcessor.sample(time);
  }

  public final Flux<Message> getMessagesflux(long quantity) {
    return senderProcessor.take(quantity);
  }

  public void startFluxFor(final MessageFactory mf) {
    RunFluxForFactory agent = new RunFluxForFactory(mf);
    agents.add(agent);
    localExecutor.execute(() -> {
      agent.run();
    });
    LOGGER.debug("messagesFlux for " + mf.getFamilyName() + " : started");
  }

}
