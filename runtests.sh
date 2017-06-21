#!/bin/sh

pip install -r ./requirements.txt -t ./libs --no-binary=:all: --upgrade
PYTHONPATH=.:./libs python2 -m unittest discover .
