package com.inmobi.messaging;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Partition Identifier of a source of data.
 *
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class Partition implements Serializable {

  private final String partitionId;
}
