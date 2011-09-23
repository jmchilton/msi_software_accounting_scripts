#!/usr/bin/env python
import sys
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta
import time

# ~/parse_collectl.py --date 20110829 --directory /project/collectl/itasca --host_prefix itasca

# TODO: Descend directories

def touch(file_name):
  parent_directory = os.path.dirname(file_name)
  os.makedirs(parent_directory)
  with file(file_name, 'a'):
    os.utime(file_name, None)

def to_postgres_date(time_object):
  """
  >>> time_tuple = CollectlSummary.parse_timestamp('20110831 15:29:59')
  >>> to_postgres_date(time_tuple)
  '2011-08-31 15:29:59'
  >>> to_postgres_date(datetime(*time_tuple[0:6]))
  '2011-08-31 15:29:59'
  """
  time_tuple = time_object
  if isinstance(time_object, datetime):
    time_tuple = time_object.timetuple()
  return time.strftime("%Y-%m-%d %H:%M:%S", time_tuple)


class CollectlCommandLineBuilder:

  def get(self, rawp_path):
    """
    Abstract this out of CollectlExecutor to allow for the unit
    testing of CollectlExecutor without collectl being installed via
    mocking this class.

    >>> builder = CollectlCommandLineBuilder()
    >>> builder.get("/project/collectl/itsaca/node0506-20110819-000100.rawp.gz")
    'collectl -sZ -P -p /project/collectl/itsaca/node0506-20110819-000100.rawp.gz'
    """
    return "collectl -sZ -P -p %s" % rawp_path

class TestCommandLineBuilder:

  def get(self, rawp_path): 
    return """echo Hello World """

class DateCutoffType:
  none, both, start, end = range(4)

class CollectlExecutor:
  """
  >>> import tempfile 
  >>> stderr_file = tempfile.mkstemp()
  >>> executor = CollectlExecutor("/project/collectl/itsaca/node0506-20110819-000100.rawp.gz", stderr_file[1])
  >>> executor.collectl_command_line_builder = TestCommandLineBuilder()
  >>> executor.execute_collectl()
  >>> contents = open(executor.output_file(), "r").read()
  >>> contents.strip()
  'Hello World'
  """
  def __init__(self, rawp_file, stderr_file):
    self.rawp_file = rawp_file
    self.stderr_file = stderr_file
    self.collectl_output_file = tempfile.mkstemp()
    self.collectl_command_line_builder = CollectlCommandLineBuilder()

  def __del__(self):
    os.remove(self.output_file())

  def execute_collectl(self):
    command_line = self.collectl_command_line_builder.get(self.rawp_file)
    stdout_fileno = self.collectl_output_file[0]
    stderr_stream = open(self.stderr_file, 'w')
    proc = subprocess.Popen(command_line, shell=True, stdout=stdout_fileno, stderr=stderr_stream)
    return_code = proc.wait()
    os.close(stdout_fileno)
    stderr_stream.close()

    if return_code != 0:
      raise RuntimeError("collectl did not return a status code of 0")

  def output_file(self):
    return self.collectl_output_file[1]

  def error_file(self):
    return self.stderr_file

class TestCollectlData:
  line1 = "#Test Line"
  line2 = "20110818 00:02:00 22733 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC"
  line3 = "20110818 00:04:00 22733 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC"
  line4 = "20110818 00:04:00 22737 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/cat"
  line5 = "20110818 00:05:00 22737 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/cat"
  line6 = "20110818 00:06:00 22734 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/ls"

  @staticmethod
  def make_temp_file():
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.write("%s\n" % TestCollectlData.line1)
    temp_file.write("%s\n" % TestCollectlData.line2)
    temp_file.write("%s\n" % TestCollectlData.line3)
    temp_file.write("%s\n" % TestCollectlData.line4)
    temp_file.write("%s\n" % TestCollectlData.line5)
    temp_file.write("%s\n" % TestCollectlData.line6)
    temp_file.flush()
    return temp_file



class CollectlSummary:
  """
  >>> temp_file = TestCollectlData.make_temp_file()
  >>> parser = CollectlSummary(temp_file.name)
  >>> execution_values = parser.build()
  >>> to_postgres_date(parser.first_date_time)
  '2011-08-18 00:02:00'
  >>> to_postgres_date(parser.last_date_time)
  '2011-08-18 00:06:00'
  >>> executions = parser.get_executions()
  >>> executions['22737 31062 /bin/cat'][5]
  0
  >>> executions['22733 31062 /bin/bash'][5]
  2
  >>> executions['22734 31062 /bin/ls'][5]
  3
  """

  def __init__(self, collectl_output):
    self.collectl_output = collectl_output
    self.first_date_time = None
    self.last_date_time = None
 
  def register_date_time(self, date_time):
    if date_time is None:
      return
    if self.first_date_time is None:
      self.first_date_time = date_time
    self.last_date_time = date_time
  
  def build(self):
    self.raw_executions = {}
    with open(self.collectl_output, 'r') as file:
      for line in file:
        self.register_date_time(CollectlSummary.add_line(line, self.raw_executions))
    return self.get_executions().values()

  def get_executions(self):
    executions = self.raw_executions
    for unique_index, execution in executions.items():
      execution.append(self.border_execution(execution))
    return executions

  def border_execution(self, execution):
    """ 
    Execution occurs during first or last timestamp.
    """
    cutoff_at_start = execution[0] == self.first_date_time
    cutoff_at_end = execution[1] == self.last_date_time
    if cutoff_at_start and cutoff_at_end:
      return DateCutoffType.both
    elif cutoff_at_start:
      return DateCutoffType.start
    elif cutoff_at_end:
      return DateCutoffType.end
    else:
      return DateCutoffType.none    

  @staticmethod
  def add_line(line, executions):
    """
    >>> executions = {}
    >>> CollectlSummary.add_line(TestCollectlData.line1, executions)
    >>> executions.values()
    []
    >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line2, executions))
    '2011-08-18 00:02:00'
    >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line3, executions))
    '2011-08-18 00:04:00'
    >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line4, executions))
    '2011-08-18 00:04:00'
    >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line5, executions))
    '2011-08-18 00:05:00'
    >>> to_postgres_date(CollectlSummary.add_line(TestCollectlData.line6, executions))
    '2011-08-18 00:06:00'
    >>> to_postgres_date(executions["22733 31062 /bin/bash"][0])
    '2011-08-18 00:02:00'
    >>> to_postgres_date(executions["22733 31062 /bin/bash"][1])
    '2011-08-18 00:04:00'
    >>> executions["22733 31062 /bin/bash"][2]
    '22733'
    >>> executions["22733 31062 /bin/bash"][3]
    '31062'
    >>> executions["22733 31062 /bin/bash"][4]
    '/bin/bash'
    >>> to_postgres_date(executions["22734 31062 /bin/ls"][0])
    '2011-08-18 00:06:00'
    >>> to_postgres_date(executions["22734 31062 /bin/ls"][1])
    '2011-08-18 00:06:00'
    """

    # Skip comments
    if(line.find("#") == 0):
      return None
    
    parsed_line = CollectlSummary.parse_line(line)
    date_time = parsed_line[0]
    pid = parsed_line[1]
    uid = parsed_line[2]
    executable = parsed_line[3]
    unique_index = "%s %s %s" % (pid, uid, executable)
    if executions.has_key(unique_index):
      execution = executions.get(unique_index)
      # update guess of stop time
      execution[1] = date_time
    else:
      # start, stop, pid, uid, executable
      executions[unique_index] = ([date_time, date_time, pid, uid, executable])
    return date_time

  @staticmethod
  def parse_line(line):
    """
    
    >>> parts = CollectlSummary.parse_line("20110818 00:02:00 22733 31062 20 22686 0 S 12620 0 1756 344 88 672 1940 2 0.00 0.00 0 0:00.00 0 0 0 0 0 0 0 0 0 /bin/bash -l /var/spool/torque/mom_priv/jobs/124731.node1081.localdomain.SC")
    >>> to_postgres_date(parts[0])
    '2011-08-18 00:02:00'
    >>> parts[1].strip()
    '22733'
    >>> parts[2].strip()
    '31062'
    >>> parts[3].strip()
    '/bin/bash'
    """    
    line_parts = line.split(' ', 29)
    time_str = "%s %s" % (line_parts[0], line_parts[1]) 
    return (CollectlSummary.parse_timestamp(time_str), line_parts[2], line_parts[3], CollectlSummary.parse_program(line_parts[29]))

  @staticmethod
  def parse_timestamp(date_str):
    """
    >>> time_tuple = CollectlSummary.parse_timestamp('20110831 15:29:59')
    >>> time.mktime(time_tuple)
    1314822599.0
    """
    return time.strptime(date_str, '%Y%m%d %H:%M:%S')

    
  @staticmethod
  def parse_program(command_line):
    return command_line.split(None, 1)[0]

class CollectlSummaryFactory:
  
  def build_for(self, output_file):
    summary = CollectlSummary(output_file)
    return summary.build()

class MockCollectlSummaryFactory:
 
  def __init__(self, expected_output_file):
    self.expected_output_file = expected_output_file

  def build_for(self, output_file):
    assert self.expected_output_file == output_file
    temp_file = TestCollectlData.make_temp_file()
    parser = CollectlSummary(temp_file.name)
    return parser.build()

class CollectlSqlDumper:
  """
  >>> import StringIO
  >>> def dump(e): output = StringIO.StringIO(); sql_dumper = CollectlSqlDumper(output); sql_dumper.dump(e, 'itasca0001'); return output.getvalue()
  >>> output = dump([(CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/bin/cat', 0)])
  >>> output.strip() #doctest: +NORMALIZE_WHITESPACE
  "INSERT INTO COLLECTL_EXECUTIONS (START, END, PID, UID, EXECUTABLE, HOST) VALUES ('2011-08-18 00:02:00', '2011-08-18 00:04:00', 123, 456, '/bin/cat', 'itasca0001');"
  >>> execution = [CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/bin/cat', 1]
  >>> output = dump([execution])
  >>> output.find('''INSERT INTO COLLECTL_EXECUTIONS (START, END, PID, UID, EXECUTABLE, HOST) SELECT '2011-08-18 00:02:00', '2011-08-18 00:04:00', 123, 456, '/bin/cat', 'itasca0001' WHERE ''') >= 0
  True
  >>> output.find('''UPDATE COLLECTL_EXECUTIONS SET START = '2011-08-18 00:02:00' WHERE ID IN (SELECT ID FROM ''') >= 0
  True
  >>> output.find('''UPDATE COLLECTL_EXECUTIONS SET END = '2011-08-18 00:04:00' WHERE ID IN (SELECT ID FROM ''') >= 0
  True
  >>> execution[5] = 2 # just start
  >>> output = dump([execution])
  >>> output.find('''SET START''') >= 0
  False
  >>> output.find('''SET END''') >= 0
  True
  >>> execution[5] = 3 # update END DATE if needed
  >>> output = dump([execution])
  >>> output.find('''SET START''') >= 0
  True
  >>> output.find('''SET END''') >= 0
  False
  """

  def __init__(self, stream = sys.stdout):
    self.stream = stream
    
  def dump(self, executions, host):
    for execution in executions:
      self.dump_execution(execution, host)

  def dump_execution(self, execution, host):
    start = execution[0]
    postgres_start = to_postgres_date(start)
    end = execution[1]
    postgres_end = to_postgres_date(end)
    pid = execution[2]
    uid = execution[3]
    executable = execution[4]
    cutoff_type = execution[5]
    if cutoff_type == DateCutoffType.none:
      insert_template = "INSERT INTO COLLECTL_EXECUTIONS (START, END, PID, UID, EXECUTABLE, HOST) VALUES ('%s', '%s', %s, %s, '%s', '%s');"
      insert_statement = (insert_template % (postgres_start, postgres_end, pid, uid, executable, host))
      print >> self.stream, insert_statement
    else:
      same_execution_condition = CollectlSqlDumper.same_execution_condition(execution, host)
      insert_template = "INSERT INTO COLLECTL_EXECUTIONS (START, END, PID, UID, EXECUTABLE, HOST) SELECT '%s', '%s', %s, %s, '%s', '%s' WHERE 1 NOT IN (SELECT 1 FROM COLLECTL_EXECUTIONS WHERE '%s');"
      insert_statement =  (insert_template % (postgres_start, postgres_end, pid, uid, executable, host,  same_execution_condition))
      print >> self.stream, insert_statement
      if cutoff_type == DateCutoffType.end or cutoff_type == DateCutoffType.both:
        update_start_template = "UPDATE COLLECTL_EXECUTIONS SET START = '%s' WHERE ID IN (SELECT ID FROM COLLECTL_EXECUTIONS WHERE '%s' AND START > '%s');"
        update_start_statement = update_start_template % (postgres_start, same_execution_condition, postgres_start)
        print >> self.stream, update_start_statement
      if cutoff_type == DateCutoffType.start or cutoff_type == DateCutoffType.both:
        update_end_template = "UPDATE COLLECTL_EXECUTIONS SET END = '%s' WHERE ID IN (SELECT ID FROM COLLECTL_EXECUTIONS WHERE '%s' AND END < '%s');"
        update_end_statement = update_end_template % (postgres_end, same_execution_condition, postgres_end)
        print >> self.stream, update_end_statement
      
  @staticmethod
  def same_execution_condition(execution, host):
    """
    >>> execution = [CollectlSummary.parse_timestamp('20110818 00:02:00'), CollectlSummary.parse_timestamp('20110818 00:04:00'), 123, 456, '/bin/cat', True]
    >>> condition = CollectlSqlDumper.same_execution_condition(execution, 'itasca0001')
    >>> condition
    "HOST = 'itasca0001' AND PID = '123' AND UID = '456' AND EXECUTABLE = '/bin/cat' AND START > '2011-08-04 00:02:00' AND END < '2011-09-01 00:04:00'"
    """
    start = execution[0]
    end = execution[1]
    pid = execution[2]
    uid = execution[3]
    executable = execution[4]

    same_execution_condition_template = "HOST = '%s' AND PID = '%s' AND UID = '%s' AND EXECUTABLE = '%s' AND START > '%s' AND END < '%s'"
    start_datetime = datetime(*start[0:6])
    end_datetime = datetime(*end[0:6])
    one_week = timedelta(weeks=2)
    condition = same_execution_condition_template % (host, pid, uid, executable, to_postgres_date(start_datetime - one_week), to_postgres_date(end_datetime + one_week))
    return condition

def parsing_started_file(log_file_path_prefix):
  return log_file_path_prefix + "-parsing-started"

def parsing_completed_file(log_file_path_prefix):
  return log_file_path_prefix + "-parsing-completed"

def stderr_file(log_file_path_prefix):
  return log_file_path_prefix + "-stderr"

class CollectlFileScanner:
  """
  
  """
  def __init__(self, node_name, rawp_file, log_file_base):
    self.node_name = node_name
    self.rawp_file = rawp_file
    self.collectl_executor = CollectlExecutor(rawp_file, stderr_file(log_file_base))
    self.collectl_summary_factory = CollectlSummaryFactory()
    self.collectl_sql_dumper = CollectlSqlDumper()
    self.log_file_base = log_file_base

  def log_start():
    touch(parsing_started_file(self.log_file_base))

  def log_end():
    touch(parsing_completed_file(self.log_file_base))

  def execute(self):
    log_start()
    self.collectl_executor.execute_collectl()
    collectl_output_file = self.collectl_executor.output_file()
    executions = self.collectl_summary_factory.build_for(collectl_output_file)
    self.collectl_sql_dumper.dump(executions, self.node_name)
    log_end()

class CollectlDirectoryScanner:
  """
  >>> import tempfile, shutil, os
  >>> temp_date_dir = tempfile.mkdtemp()
  >>> temp_log_dir = tempfile.mkdtemp()
  >>> open(temp_date_dir + "/node0506-20110819-000100.rawp.gz", 'w').close()
  >>> open(temp_date_dir + "/node0506-20110819-000100.raw.gz", 'w').close()
  >>> open(temp_date_dir + "/node0507-20110819-000100.rawp.gz", 'w').close()
  >>> open(temp_date_dir + "/node0507-20110819-000100.raw.gz", 'w').close()
  >>> itasca_log_dir = os.path.join(temp_log_dir, "itasca")
  >>> os.makedirs(itasca_log_dir)
  >>> open(os.path.join(itasca_log_dir, "node0507-20110819-000100.rawp.gz-parsing-completed"), 'w').close() # Mark file as previously processed
  >>> dated_dir_scanner = CollectlDirectoryScanner('20110819', temp_date_dir, temp_log_dir, 'itasca')
  >>> nodes = dated_dir_scanner.get_node_scanners()
  >>> file_scanner = nodes.next()
  >>> file_scanner.node_name
  'itascanode0506'
  >>> nodes.next()
  Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
  StopIteration
  >>> open(temp_date_dir + "/node0506-20110820-000100.rawp.gz", 'w').close()
  >>> open(temp_date_dir + "/node0506-20110820-000100.raw.gz", 'w').close()
  >>> undated_dir_scanner = CollectlDirectoryScanner(None, temp_date_dir, temp_log_dir, 'itasca')
  >>> nodes = undated_dir_scanner.get_node_scanners()
  >>> files = [os.path.basename(nodes.next().rawp_file), os.path.basename(nodes.next().rawp_file)]
  >>> files.sort()
  >>> files
  ['node0506-20110819-000100.rawp.gz', 'node0506-20110820-000100.rawp.gz']
  >>> shutil.rmtree(temp_date_dir)
  >>> shutil.rmtree(temp_log_dir)
  """

  def get_node_scanners(self):
    for dir_file in os.listdir(self.directory):
      if self.__do_parse_file(dir_file):
        yield self.__build_file_parser(dir_file)

  def execute(self):
    for file_parser in self.get_node_scanners():
      file_parser.execute()

  def __do_parse_file(self, dir_file):
    return self.__filename_matches(dir_file) and not self.__previously_parsed(dir_file)

  def __filename_matches(self, dir_file):
    date_match = "\w+"
    if has_text(self.date):
      date_match = self.date
    do_parse = re.match("^\w+-%s-\w+.rawp.gz$" % date_match, dir_file)
    return do_parse

  def __log_file_base(self, dir_file):
    return os.path.join(os.path.join(self.log_directory, self.host_prefix), dir_file)

  def __previously_parsed(self, dir_file):
    return os.path.exists(parsing_completed_file(self.__log_file_base(dir_file)))

  def __node_name(self, rawp_file):
    return "%s%s" % (self.host_prefix, rawp_file.split("-")[0])

  def __build_file_parser(self, rawp_file):
    return CollectlFileScanner(self.__node_name(rawp_file), os.path.join(self.directory, rawp_file), self.__log_file_base(rawp_file))
    
  def __init__(self, date, directory, log_directory, host_prefix):
    self.date = date
    self.directory = directory
    self.host_prefix = host_prefix
    self.log_directory = log_directory
  

def has_text(input):
  """
  >>> not has_text('')
  True
  >>> not has_text(None)
  True
  >>> not has_text('Hello World!')
  False
  """  
  return not input is None and not input == ''

def main():
  from optparse import OptionParser
  parser = OptionParser()
  parser.add_option("--date", dest="date", help="Date in format YYYYMMDD", required = False)
  parser.add_option("--host_prefix", dest="host_prefix", help="Host name prefix (e.g. itasca)")
  parser.add_option("--directory", dest="directory", help="Directory to scan for collectl files")
  parser.add_option("--log_directory", dest="log_directory", help="Directory to record information about which files have been processed.", default="collectl_parse_log")
  (options, args) = parser.parse_args()
  date = options.date
  host_prefix = options.host_prefix
  directory = options.directory
  if not has_text(host_prefix) or not has_text(directory):
    parser.error("Incorrect arguments")
  
  CollectlDirectoryScanner(date = date, directory = directory, log_directory = log_directory, host_prefix = host_prefix).execute()


if __name__ == "__main__":
  main()
