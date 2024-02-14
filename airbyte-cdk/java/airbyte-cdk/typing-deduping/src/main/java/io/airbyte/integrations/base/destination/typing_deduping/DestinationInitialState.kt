/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.base.destination.typing_deduping

@JvmRecord
data class DestinationInitialState<DestinationState>(val streamConfig: StreamConfig,
                                                     val isFinalTablePresent: Boolean,
                                                     val initialRawTableState: InitialRawTableState,
                                                     val isSchemaMismatch: Boolean,
                                                     val isFinalTableEmpty: Boolean,
                                                     val destinationState: DestinationState)
