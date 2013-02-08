package com.inmobi.messaging.consumer.audit;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class GroupBy {


  class Group {
    private Map<Column, String> columns;

    public Group(Map<Column, String> values) {
      this.columns = values;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      Iterator<Column> iterator = isSet.iterator();
      while (iterator.hasNext()) {
        Column column = iterator.next();
        result = prime
            * result
            + ((columns.get(column) == null) ? 0 : columns.get(column)
                .hashCode());
      }
      return result;
    }

    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append("Group [");
      Iterator<Column> iterator = isSet.iterator();
      while (iterator.hasNext()) {
        Column column = iterator.next();
        buffer.append(column + " = " + columns.get(column) + ",");
      }
      buffer.append("]");
      return buffer.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Group other = (Group) obj;
      Iterator<Column> iterator = isSet.iterator();
      while (iterator.hasNext()) {
        Column column = iterator.next();
        if (columns.get(column) == null) {
          if (other.columns.get(column) != null)
            return false;
        } else if (!columns.get(column).equals(other.columns.get(column)))
          return false;
      }
      return true;
    }

  }

  private Set<Column> isSet;

  GroupBy(String input) {
    isSet = new HashSet<Column>();
    if (input == null)
      return;
    String[] columns = input.split(",");
    for (String s : columns) {
      isSet.add(Column.valueOf(s.toUpperCase()));
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((isSet == null) ? 0 : isSet.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GroupBy other = (GroupBy) obj;
    if (isSet == null) {
      if (other.isSet != null)
        return false;
    } else if (!isSet.equals(other.isSet))
      return false;
    return true;
  }


  public Group getGroup(Map<Column, String> values) {
    return new Group(values);
  }

}
