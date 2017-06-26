/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.app.etl.dataset.DatasetAccessApp;
import co.cask.cdap.app.etl.dataset.TPFSService;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

//import co.cask.cdap.app.etl.StreamTPFSWithProjectionTest;

/**
 * Tests functionalities of TPFS parquet sinks and sources
 */
public class TPFSSinkSourceTest extends ETLTestBase {

  private static final String SOURCE_STREAM = "sourceStream";
  public static final Schema TPFS_SCHEMA = Schema.recordOf(
    "etlSchemaBody",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testMR() throws Exception {
    testStreamTPFSWithProjection(Engine.MAPREDUCE);
  }

  @Test
  public void testSpark() throws Exception {
    testStreamTPFSWithProjection(Engine.SPARK);
  }


  public void testStreamTPFSWithProjection(Engine engine) throws Exception {
    //1. create a source stream and send an event
    StreamId sourceStreamId = createSourceStream(SOURCE_STREAM);
    streamClient.sendEvent(sourceStreamId, DUMMY_STREAM_EVENT);

    //2. Deploy an application with a service to get TPFS data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(TPFSService.class.getSimpleName());
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // 3. Run Stream To TPFS with Projection Transform pipeline
    ApplicationId streamToTPFSAppId = TEST_NAMESPACE.app("StreamToTPFSWithProjection");
    ETLBatchConfig etlBatchConfig = constructStreamToTPFSConfig(engine);
    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlBatchConfig);
    ApplicationManager appManager = getTestManager().deployApplication(streamToTPFSAppId, appRequest);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);

    long timeInMillis = System.currentTimeMillis();
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // 4. Run TPFS to TPFS pipeline where the source is the sink from the above pipeline
    ApplicationId tpfsToTPFSAppId = TEST_NAMESPACE.app("TPFSToTPFSWithProjection");
    etlBatchConfig = constructTPFSToTPFSConfig(engine);
    appRequest = getBatchAppRequestV2(etlBatchConfig);
    appManager = getTestManager().deployApplication(tpfsToTPFSAppId, appRequest);
    workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);

    // add 5 minutes to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("runtime", String.valueOf(timeInMillis)));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // both the pipelines needs to run first so that the TPFS gets created and the service can access it.

    // 5. Verify data in TPFS
    verifyTPFSData(serviceManager, TPFSService.TPFS_1);
    verifyTPFSData(serviceManager, TPFSService.TPFS_2);
  }

  private void verifyTPFSData(ServiceManager serviceManager, String tpfsName)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    URL tpfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                          TPFSService.TPFS_PATH + String.format("/%s?time=1", tpfsName));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, tpfsURL, getClientConfig().getAccessToken());
    List<IntegrationTestRecord> responseObject = ObjectResponse.<List<IntegrationTestRecord>>fromJsonBody(
      response, new TypeToken<List<IntegrationTestRecord>>() {
      }.getType()).getResponseObject();
    Assert.assertEquals("AAPL", responseObject.get(0).getTicker());

  }

  private ETLBatchConfig constructStreamToTPFSConfig(Engine engine) {

    ETLStage source = etlStageProvider.getStreamBatchSource(SOURCE_STREAM, "10m", "0d",
                                                            Formats.CSV, DUMMY_STREAM_EVENT_SCHEMA, "|");
    ETLStage sink = new ETLStage("sink",
                                 new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE,
                                               ImmutableMap.of("schema", TPFSService.EVENT_SCHEMA.toString(),
                                                               "name", TPFSService.TPFS_1),
                                               null));
    ETLStage transform = new ETLStage("testTransform",
                                      new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                    ImmutableMap.of("drop", "headers"), null));
    return ETLBatchConfig.builder("*/10 * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .setEngine(engine)
      .build();
  }

  private ETLBatchConfig constructTPFSToTPFSConfig(Engine engine) {

    ETLStage source = new ETLStage("source",
                                   new ETLPlugin("TPFSParquet", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of("name", TPFSService.TPFS_1,
                                                                 "schema", TPFSService.EVENT_SCHEMA.toString(),
                                                                 "duration", "20m"),
                                                 null));
    ETLStage sink = new ETLStage("sink",
                                 new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE,
                                               ImmutableMap.of("name", TPFSService.TPFS_2,
                                                               "schema", TPFSService.EVENT_SCHEMA.toString()),
                                               null));

    return ETLBatchConfig.builder("0 * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(engine)
      .build();
  }

  private class IntegrationTestRecord {
    private final long ts;
    private final String ticker;
    private final double price;
    private final int num;

    IntegrationTestRecord(long ts, String ticker, double price, int num) {
      this.ticker = ticker;
      this.ts = ts;
      this.price = price;
      this.num = num;
    }

    public String getTicker() {
      return ticker;
    }
  }
}

