/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.source.relationaldb.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateStats;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SourceStateIteratorTest {

  SourceStateIteratorManager mockProcessor;
  Iterator<AirbyteMessage> messageIterator;
  ConfiguredAirbyteStream stream;

  SourceStateIterator sourceStateIterator;

  @BeforeEach
  void setup() {
    mockProcessor = mock(SourceStateIteratorManager.class);
    stream = mock(ConfiguredAirbyteStream.class);
    messageIterator = mock(Iterator.class);
    sourceStateIterator = new SourceStateIterator(messageIterator, stream, mockProcessor);
  }

  // Provides a way to generate a record message and will verify corresponding spied functions have
  // been called.
  void processRecordMessage() {
    doReturn(true).when(messageIterator).hasNext();
    doReturn(false).when(mockProcessor).shouldEmitStateMessage(anyLong(), any());
    AirbyteMessage message = new AirbyteMessage().withType(Type.RECORD).withRecord(new AirbyteRecordMessage());
    doReturn(message).when(mockProcessor).processRecordMessage(eq(stream), any());
    doReturn(message).when(messageIterator).next();

    assertEquals(message, sourceStateIterator.computeNext());
    verify(mockProcessor, atLeastOnce()).processRecordMessage(eq(stream), eq(message));
    verify(mockProcessor, atLeastOnce()).shouldEmitStateMessage(eq(0L), any());
  }

  @Test
  void testShouldProcessRecordMessage() {
    processRecordMessage();
  }

  @Test
  void testShouldEmitStateMessage() {
    processRecordMessage();
    doReturn(true).when(mockProcessor).shouldEmitStateMessage(anyLong(), any());
    final AirbyteStateMessage stateMessage = new AirbyteStateMessage();
    doReturn(stateMessage).when(mockProcessor).generateStateMessageAtCheckpoint(stream);
    AirbyteMessage expectedMessage = new AirbyteMessage().withType(Type.STATE).withState(stateMessage);
    expectedMessage.getState().withSourceStats(new AirbyteStateStats().withRecordCount(1.0));
    assertEquals(expectedMessage, sourceStateIterator.computeNext());
  }

  @Test
  void testShouldEmitFinalStateMessage() {
    processRecordMessage();
    processRecordMessage();
    doReturn(false).when(messageIterator).hasNext();
    final AirbyteStateMessage stateMessage = new AirbyteStateMessage();
    doReturn(stateMessage).when(mockProcessor).createFinalStateMessage(stream);
    AirbyteMessage expectedMessage = new AirbyteMessage().withType(Type.STATE).withState(stateMessage);
    expectedMessage.getState().withSourceStats(new AirbyteStateStats().withRecordCount(2.0));
    assertEquals(expectedMessage, sourceStateIterator.computeNext());
  }

  @Test
  void testShouldSendEndOfData() {
    processRecordMessage();
    doReturn(false).when(messageIterator).hasNext();
    doReturn(new AirbyteStateMessage()).when(mockProcessor).createFinalStateMessage(stream);
    sourceStateIterator.computeNext();

    // After sending the final state, if iterator was called again, we will return null.
    assertEquals(null, sourceStateIterator.computeNext());
  }

  @Test
  void testShouldRethrowExceptions() {
    processRecordMessage();
    doThrow(new ArrayIndexOutOfBoundsException("unexpected error")).when(messageIterator).hasNext();
    assertThrows(RuntimeException.class, () -> sourceStateIterator.computeNext());
  }

}
