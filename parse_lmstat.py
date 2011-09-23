#!/usr/bin/env python
import sys
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta
import time

def to_postgres_date(time_object):
  """
  >>> time_tuple = (2011, 8, 31, 15, 29, 59, 0, 0, 0)
  >>> to_postgres_date(time_tuple)
  '2011-08-31 15:29:59'
  >>> to_postgres_date(datetime(*time_tuple[0:6]))
  '2011-08-31 15:29:59'
  """
  time_tuple = time_object
  if isinstance(time_object, datetime):
    time_tuple = time_object.timetuple()
  return time.strftime("%Y-%m-%d %H:%M:%S", time_tuple)

class TestLmstatExecutor:
  """
  >>> output = TestLmstatExecutor().execute()
  >>> output.find('Feature usage info') > -1
  True
  """
  
  def execute(self):
    return open('test-data/example_lmstat_output.txt', 'r').read()

class LmstatExecutor:

  def execute(self):
    output_file = tempfile.mkstemp()
    stdout_fileno = output_file[0]
    proc = subprocess.Popen(command_line, shell=True, stdout=stdout_fileno)
    return_code = proc.wait()
    os.close(stdout_fileno)
    if return_code != 0:
      raise RuntimeError("lmstat did not return a status code of 0")
    return open(output_file[1], 'r').read()

class LmstatUserSnapshot:
  
  def __init__(self, username, host, licenses):
    self.username = username
    self.host = host
    self.licenses = licenses

class LmstatAppSnapshot:
  
  def add_user_snapshot(self, line):
    match = re.search("    (\w+)\s+(\w+).*", line)
    num_licenses = 1
    num_match = re.search("(\d+) licenses", line)
    if num_match is not None:
      num_licenses = int(num_match.group(1))

    self.user_snapshots.append(LmstatUserSnapshot(match.group(1), match.group(2), num_licenses))

  def __init__(self, datetime, lines, parse_from):
    first_line = lines[parse_from]
    match = re.search('Users of (.*): .*(\d+) license.*(\d+) license', first_line)
    self.feature = match.group(1)
    self.total_licenses = int(match.group(2))
    self.used_licenses = int(match.group(3))
    
    if self.used_licenses == 0:
      return
    
    second_line = lines[parse_from + 1]
    match = re.search("  \".*\" v.*, vendor: (\w+)", second_line)
    self.vendor = match.group(1)
    self.user_snapshots = []
    index = parse_from + 1
    while True:
      if index > len(lines):
        break
      line = lines[index]
      if line[0] != ' ':
        break
      if line[0:4] == '    ':
        self.add_user_snapshot(line)
      index = index + 1

class LmstatParser:
  """
  >>> parser = LmstatParser()
  >>> parser.executor = TestLmstatExecutor()
  >>> parser.parse()
  >>> parser.datetime
  '2011-09-22 08:34:00'
  >>> first_snapshot = parser.app_snapshots[0]
  >>> first_snapshot.feature
  'moe'
  >>> first_snapshot.total_licenses
  6
  >>> first_snapshot.used_licenses
  3
  >>> first_snapshot.vendor
  'chemcompd'
  >>> first_user_snapshot = first_snapshot.user_snapshots[0]
  >>> first_user_snapshot.username
  'chilton'
  >>> first_user_snapshot.host
  'vl5'
  >>> first_user_snapshot.licenses
  3
  """

  def __init__(self):
    self.executor = LmstatExecutor()

  def parse(self):
    output = self.executor.execute()
    
    datetime_match = re.search("Flexible License Manager status on \w+ ([\d/]+\s+[\d\:]+)\s*", output)
    self.datetime = to_postgres_date(time.strptime(datetime_match.group(1), '%m/%d/%Y %H:%M'))

    lines = [line for line in output.split("\n") if re.match("$\s*^", line) is None]
    start_indicies = [index for (str, index) in zip(lines, range(len(lines))) if str.find('Users of') == 0]

    app_snapshots = []
    for start_index in start_indicies:
      app_snapshots.append(LmstatAppSnapshot(self.datetime, lines, start_index))

    self.app_snapshots = app_snapshots

def main():
  from optparse import OptionParser
  parser = OptionParser()
  (options, args) = parser.parse_args()
  LmstatParser().parse()

  
if __name__ == "__main__":
  main()
