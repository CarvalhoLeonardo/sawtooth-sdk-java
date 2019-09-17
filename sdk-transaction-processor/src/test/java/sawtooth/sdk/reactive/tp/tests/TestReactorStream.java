package sawtooth.sdk.reactive.tp.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import sawtooth.sdk.protobuf.Message;
import sawtooth.sdk.reactive.tp.fake.FakeValidator;
import sawtooth.sdk.reactive.tp.message.flow.ReactorStream;
import sawtooth.sdk.reactive.tp.processor.TransactionHandler;
import sawtooth.sdk.reactive.tp.simulator.SimpleTestTransactionHandler;

/**
 *
 * @author Leonardo T. de Carvalho
 *
 * <a href="https://github.com/CarvalhoLeonardo">GitHub</a>
 * <a href="https://br.linkedin.com/in/leonardocarvalho">LinkedIn</a>
 *
 * mvn -Dtest=TestReactorStream -Dmaven.surefire.debug="-Xdebug
 * -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 -Xnoagent -Djava.compiler=NONE"
 * clean test
 *
 *
 */

@Test
public class TestReactorStream extends BaseTest {
  private static String ADDRESS = "ipc://backend.ipc";
  static Random rand = new Random(System.nanoTime());
  static TransactionHandler simplTH = new SimpleTestTransactionHandler();
  private static final long waitingTimeTest = 2000;
  FakeValidator fVal;

  ReactorStream reactStream;
  ExecutorService tpe;

  @BeforeClass
  public void setUp() throws InterruptedException {
    tpe = Executors.newFixedThreadPool(2);
    fVal = new FakeValidator(simplTH, ADDRESS, parallelFactor);
    reactStream = new ReactorStream(ADDRESS, parallelFactor + 2);

    LOGGER.debug("Preparing to start Validator...");
    Future<?> startVal = tpe.submit(() -> {
      fVal.run();
    });

    try {
      startVal.get();
      LOGGER.debug("Validator Started.");
      LOGGER.debug("Preparing to start Stream...");
      Future<?> startStream = tpe.submit(() -> {
        reactStream.run();
      });
      startStream.get();
      LOGGER.debug("Stream Started.");
    } catch (ExecutionException e) {
      LOGGER.error("Something wrong : " + e.getMessage());
      e.printStackTrace(System.err);
    }
    LOGGER.debug("Set up!");
  }

  private boolean testMany(int howMany) throws InvalidProtocolBufferException, InterruptedException,
      ExecutionException, TimeoutException {
    List<Message> allToSend = new ArrayList<>();

    final List<CompletableFuture<Message>> allToReceive = Collections
        .synchronizedList(new ArrayList<>());
    List<String> allExpectedIDs = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < howMany; i++) {
      Message toSend = simplTH.getCoreMessageFactory().getPingRequest();
      allToSend.add(toSend);
      allExpectedIDs.add(toSend.getCorrelationId());
      LOGGER.debug("Created request : " + toSend.getCorrelationId());
    }

    allToSend.stream().map(em -> {
      LOGGER.debug("Sending request : " + em.getCorrelationId());
      reactStream.sendBack(em.getCorrelationId(), em);
      try {
        CompletableFuture<Message> awaitingOne;

        awaitingOne = (CompletableFuture<Message>) reactStream.receive(em.getCorrelationId(),
            Duration.ofSeconds(1L));
        awaitingOne.whenComplete((rs, ex) -> {
          if (ex != null) {
            LOGGER.debug("::==========>> FAILURE {}.", ex.getMessage());
          } else {
            LOGGER.debug("::==========>> Received {}.", rs.getCorrelationId());
          }

        });
        allToReceive.add(awaitingOne);
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
      return true;
    }).allMatch(al -> true);

    CompletableFuture<?>[] stupidRules = new CompletableFuture<?>[allToReceive.size()];
    allToReceive.toArray(stupidRules);
    CompletableFuture<Void> result = CompletableFuture.allOf(stupidRules);
    result.get(waitingTimeTest, TimeUnit.MILLISECONDS);

    assertFalse(allExpectedIDs.isEmpty());
    assertFalse(allToReceive.isEmpty());

    List<Boolean> allResults = allToReceive.stream().map(er -> {
      assertTrue(er.isDone());
      Message received = null;
      try {
        received = er.get();
        assertNotNull(received);
        LOGGER.debug("Testing correlation ID " + received.getCorrelationId());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        return false;
      }
      assertTrue(allExpectedIDs.contains(received.getCorrelationId()));
      return allExpectedIDs.contains(received.getCorrelationId());
    }).collect(Collectors.toList());

    LOGGER.debug("Results count : " + allResults.size() + " from " + howMany);
    LOGGER.debug("Results " + Arrays.deepToString(allResults.toArray()));

    return (allResults.stream().allMatch(er -> Boolean.TRUE.equals(er))
        && allResults.size() == howMany);
  }

  @Test
  public void testSendCoresPlusOne() throws InvalidProtocolBufferException, TimeoutException,
      InterruptedException, ExecutionException {
    int cores = Runtime.getRuntime().availableProcessors();
    LOGGER.debug("Found {} cores", cores);
    assertTrue(testMany(cores + 1));

  }

  @Test(dependsOnMethods = { "testSendTen" })
  public void testSendHundred() throws InvalidProtocolBufferException, TimeoutException,
      InterruptedException, ExecutionException {
    assertTrue(testMany(100));
  }

  @Test
  public void testSendOne() throws InvalidProtocolBufferException, TimeoutException,
      InterruptedException, ExecutionException {
    assertTrue(testMany(1));
  }

  @Test
  public void testSendTen() throws InvalidProtocolBufferException, TimeoutException,
      InterruptedException, ExecutionException {
    assertTrue(testMany(10));
  }

  @Test
  public void testSendTwo() throws InvalidProtocolBufferException, TimeoutException,
      InterruptedException, ExecutionException {
    assertTrue(testMany(2));
  }

}
