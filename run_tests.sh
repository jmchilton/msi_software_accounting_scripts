#!/bin/bash

rm htmlcov/*
rm .coverage
rm nosetests.xml

nosetests parse_collectl.py

# Convert to UTF-8, Java XML parser chokes on it otherwise.
iconv -f iso-8859-1 -t utf-8 nosetests.xml > nosetests_utf8.xml
mv nosetests_utf8.xml nosetests.xml
