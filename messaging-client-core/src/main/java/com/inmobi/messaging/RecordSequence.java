package com.inmobi.messaging;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Sequence Holding object for data in a source. Sequence number is expected to be a monotonically increasing value.
 *
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class RecordSequence implements Serializable {

  private final long sequenceNumber;
}
