package sawtooth.sdk.rest.client.tests;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.enterprise.inject.spi.CDI;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import sawtooth.sdk.reactive.rest.model.RESTEntry;
import sawtooth.sdk.reactive.rest.model.RESTState;
import sawtooth.sdk.reactive.rest.ops.RESTStateOps;

@Test
public class TestListState extends BaseTest {

  RESTState result;
  RESTStateOps underTest;

  @BeforeClass
  public void getCDIBeans() throws ClassNotFoundException {
    underTest = CDI.current().select(RESTStateOps.class).get();
  }

  @Test
  public void testCDI() {
    assertNotNull(underTest);
  }

  @Test(dependsOnMethods = { "testCDI" })
  public void testListAll() throws InterruptedException, ExecutionException {
    Future<RESTState> futureResult = underTest.getAllStates(null, null, null, 100, false);
    assertNotNull(futureResult);
    assertFalse(((CompletableFuture<RESTState>) futureResult).isCompletedExceptionally());
    result = futureResult.get();
    assertNotNull(result);
    LOGGER.debug("Got States {}...", result);
    assertTrue(futureResult.isDone());
  }

  @Test(dependsOnMethods = { "testListAll" })
  public void testQueryEachState() {

    result.getData().forEach(ee -> {
      Future<RESTEntry> futureResult;
      try {
        futureResult = underTest.getOneState(ee.getAddress(), null);
        assertNotNull(futureResult);
        assertFalse(((CompletableFuture<RESTEntry>) futureResult).isCompletedExceptionally());
        RESTEntry entryResult = futureResult.get();
        assertNotNull(entryResult);
        assertTrue(futureResult.isDone());
        LOGGER.debug("Got State for address {} as {} ...", ee, entryResult);
      } catch (Exception e) {
        fail("Getting address " + ee.getAddress() + " failed : " + e.getMessage());
        e.printStackTrace();
      }

    });

  }

}
