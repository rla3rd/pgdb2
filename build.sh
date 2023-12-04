#!/bin/bash
cd $HOME/Dropbox/projects/pgdb2
$PYTHON setup.py build
$PYTHON setup.py sdist
$PYTHON setup.py install
