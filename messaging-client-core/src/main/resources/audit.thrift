#!/usr/local/bin/thrift --gen cpp:pure_enums --gen java

namespace java com.inmobi.audit.thrift
namespace cpp  audit.thrift

struct AuditPacket
{
  1: i64 timestamp,
  2: string topic,
  3: string tier,
  4: string hostname,
  5: i32 windowSizeInMins,
  6: map<i64,i64> received,
  7: map<i64,i64> sent
}