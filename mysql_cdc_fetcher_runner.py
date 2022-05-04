import json
import boto3

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)

def main():
  kinesis = boto3.client("kinesis",region_name='{}')
  stream = BinLogStreamReader(
    connection_settings= {
      "host": "{}",
      "port":{} ,
      "user": "{}",
      "passwd": "{}"},
    server_id=100,
    blocking=True,
    resume_stream=True,
    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
  for binlogevent in stream:
    for row in binlogevent.rows:
      event = {"schema": binlogevent.schema,
      "table": binlogevent.table,
      "type": type(binlogevent).__name__,
      "row": row
      }
      kinesis.put_record(StreamName="{}", Data=str(event), PartitionKey="1")
      print(json.dumps(event))
      
main()