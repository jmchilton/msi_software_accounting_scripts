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

def main():
  from optparse import OptionParser
  parser = OptionParser()
  (options, args) = parser.parse_args()



if __name__ == "__main__":
  main()
