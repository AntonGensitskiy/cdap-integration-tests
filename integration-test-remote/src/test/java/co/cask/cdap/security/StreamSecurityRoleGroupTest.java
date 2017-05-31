package co.cask.cdap.security;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static co.cask.cdap.proto.security.Principal.PrincipalType.GROUP;
import co.cask.cdap.security.spi.authorization.RoleAlreadyExistsException;
import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/*
 * Copyright © 2015 Cask Data, Inc.
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

public class StreamSecurityRoleGroupTest extends AudiTestBase {
  private static final StreamId NONEXISTENT_STREAM = TEST_NAMESPACE.stream("nonexistentStream");
  private static final StreamId STREAM_NAME = TEST_NAMESPACE.stream("streamTest");

  private static final String ADMIN_USER = "cdapitn";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";
  private static final String CAROL = "carol";
  private static final String EVE = "eve";
  private static final String PASSWORD_SUFFIX = "password";
  private static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";
  private static final String ROLE_READ = "role_read";
  private static final String ROLE_WRITE = "role_write";
  private static final String NSCREATOR = "nscreator";

  // This is to work around https://issues.cask.co/browse/CDAP-7680
  // Where we don't delete privileges when a namespace is deleted.
  private static String generateRandomName() {
    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
    return new BigInteger(130, new SecureRandom()).toString(32);
  }

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException, TimeoutException, Exception {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  /**
   * SEC-AUTH-019(STREAM) and Group and Role's version of SEC-AUTH-013
   * Grant a user WRITE access on a stream. Try to get the stream from a program and call a WRITE method on it.
   *
   * There are three users in the system.
   * Alice and Bob belong to group 'nscreator' and Eve doesn't belong to 'nscreator'.
   * Now we assign a role which has write privileges to 'nscreator' group.
   * Expected behavior would be that Alice and Bob can successfully write to stream,
   * while Eve cannot.
   *
   * @throws Exception
   */
  @Test
  public void SEC_AUTH_013() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);
    //creating stream within the namespace created
    StreamId streamId = namespaceId.stream("streamTest");
    //creating a stream using admin client
    streamAdminClient.create(streamId);
    StreamProperties config = streamAdminClient.getConfig(streamId);
    Assert.assertNotNull(config);

    //now authorize WRITE access to role_write
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    //Create write role, grant write

    Role role_write = new Role(ROLE_WRITE);
    try {
      authorizationClient.createRole(role_write);
    }catch(RoleAlreadyExistsException ex){
      //user_role already exists, it's fine to move on from here
    }
    authorizationClient.grant(namespaceId, role_write, Collections.singleton(Action.WRITE));

    //create a principal group nscreator which already exist in UNIX system and add role_write to the group
    authorizationClient.addRoleToPrincipal(role_write, new Principal(NSCREATOR, GROUP));

    //1. using the user Alice to write message on the stream, should succeed
    //create user Alice
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());
    //create alice client
    StreamClient streamAliceClient = new StreamClient(aliceConfig, aliceClient);
    streamAliceClient.sendEvent(streamId, " a b ");

    //calling a read method from admin client should generate expected result, since alice successfully write to the stream and admin can retrieve it
    List<StreamEvent> events = streamAdminClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(" a b ", Bytes.toString(events.get(0).getBody()));


    //2. using the user Bob to write message on the stream, should succeed
    //create user Bob
    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(aliceConfig);
    bobClient.addListener(createRestClientListener());
    //create bob client
    StreamClient streamBobClient = new StreamClient(bobConfig, bobClient);
    streamBobClient.sendEvent(streamId, " c d ");

    //calling a read method from admin client should generate expected result, since carol successfully write to the stream and admin can retrieve it
    events = streamAdminClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(" c d ", Bytes.toString(events.get(1).getBody()));

    //3. using the user eve to write message on the stream, should fail
    //create user Eve
    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());
    //create Eve client
    try {
      StreamClient streamEveClient = new StreamClient(eveConfig, eveClient);
      streamEveClient.sendEvent(streamId, " e f ");
      //fail if Eve has authorization to write
      Assert.fail();
    }catch(UnauthorizedException ex){
      //expected unauthorized Exception here
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * SEC-AUTH-019(STREAM) and Group and Role's version of SEC-AUTH-012
   * Grant a user READ access on a stream. Try to get the stream from a program and call a READ method on it.
   *
   * There are three users in the system.
   * Alice and Bob belong to group 'nscreator' and Eve doesn't belong to 'nscreator'.
   * Now we assign a role which has write privileges to 'nscreator' group.
   * Expected behavior would be that Alice and Bob can successfully write to stream,
   * while Eve cannot.
   *
   * @throws Exception
   */
  @Test
  public void SEC_AUTH_012() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);
    //creating stream within the namespace created
    StreamId streamId = namespaceId.stream("streamTest");
    //creating a stream using admin client
    streamAdminClient.create(streamId);
    StreamProperties config = streamAdminClient.getConfig(streamId);
    Assert.assertNotNull(config);

    //now authorize WRITE access to role_write
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    //Create write role, grant write

    Role role_read = new Role(ROLE_READ);
    try {
      authorizationClient.createRole(role_read);
    }catch(RoleAlreadyExistsException ex){
      //user_role already exists, it's fine to move on from here
    }
    authorizationClient.grant(namespaceId, role_read, Collections.singleton(Action.READ));

    //create a principal group nscreator which already exist in UNIX system and add role_write to the group
    authorizationClient.addRoleToPrincipal(role_read, new Principal(NSCREATOR, GROUP));

    //1. using the user Alice to read message on the stream, should succeed

    streamAdminClient.sendEvent(streamId, " a b ");

    //create user Alice
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());
    //create alice client
    StreamClient streamAliceClient = new StreamClient(aliceConfig, aliceClient);
    //calling a read method from admin client should generate expected result, since alice successfully write to the stream and admin can retrieve it
    List<StreamEvent> events = streamAliceClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(" a b ", Bytes.toString(events.get(0).getBody()));


    //2. using the user Bob to read message on the stream, should succeed

    streamAdminClient.sendEvent(streamId, " c d ");

    //create user Bob
    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(aliceConfig);
    bobClient.addListener(createRestClientListener());
    //create bob client
    StreamClient streamBobClient = new StreamClient(bobConfig, bobClient);

    //calling a read method from admin client should generate expected result, since carol successfully write to the stream and admin can retrieve it
    events = streamBobClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                         Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(" c d ", Bytes.toString(events.get(1).getBody()));

    //3. using the user eve to write message on the stream, should fail
    streamAdminClient.sendEvent(streamId, " e f ");
    //create user Eve
    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());
    //create Eve client
    StreamClient streamEveClient = new StreamClient(eveConfig, eveClient);


    try {
      //fail if Eve is allowed to read the stream here
      streamEveClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                Lists.<StreamEvent>newArrayList());
      Assert.fail();
    }catch(IOException ex){
      //expected IOException 403 forbidden URL access here
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  // We have to use RestClient directly to attempt to create a stream with an invalid name (negative test against the
  // StreamHandler), because the StreamClient throws an IllegalArgumentException when passing in an invalid stream name.
  private void createStream(String streamName)
    throws BadRequestException, IOException, UnauthenticatedException, UnauthorizedException {
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveNamespacedURLV3(TEST_NAMESPACE, String.format("streams/%s", streamName));
    HttpResponse response = getRestClient().execute(HttpMethod.PUT, url, clientConfig.getAccessToken(),
                                                    HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + response.getResponseBodyAsString());
    }
  }
}