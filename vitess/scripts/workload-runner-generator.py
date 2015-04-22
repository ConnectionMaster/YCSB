import json
import sys
import os

def main(unused_argv):
  vtgate_host = sys.argv[1]
  with open(os.environ['WORKLOAD_FILE']) as data_file:
    data = json.load(data_file)

  with open('workload-runner.sh','w') as cmd_file:
    for index, workload in enumerate(data["workloads"]):
      action = workload["action"]
      create_table = "'CREATE TABLE usertable (YCSB_KEY VARCHAR (255) PRIMARY KEY, field0 TEXT, field1 TEXT, keyspace_id BIGINT unsigned NOT NULL)'"
      cmd = 'YCSB/bin/ycsb %s vitess -P YCSB/workloads/workload%s -p hosts=%s -p keyspace=test_keyspace -p fieldcount=2 -p fieldcount=2 -p recordcount=%s -threads %s -s' % (
                      action, workload["workload"], vtgate_host, workload['recordcount'], workload["threads"])
      if action == 'run':
        cmd += ' -p operationcount=%s ' % workload['operationcount']
      if not (workload.has_key('createtable') and workload['createtable'] == 'True'):
        cmd += ' -p createTable=skip'
      else:
        cmd += ' -p createTable=%s' % create_table
      if workload.has_key('maxexecutiontime'):
        cmd += ' -p maxexecutiontime=%s' % workload['maxexecutiontime']
      cmd_file.write('%s > ~/workloadlogs/workload%s%02d.txt\n' % (cmd, workload["workload"], index))
      if action == 'load' and workload.has_key('wait'):
        cmd_file.write('sleep %s\n' % workload['wait'])
      # sleep for 1 min before each run
      if action == 'run':
        cmd_file.write('sleep 60\n')

if __name__ == '__main__':
  main(sys.argv)
