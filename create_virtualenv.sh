#!/bin/bash
pyenv virtualenv 3.7.4 dpa_metro_cdmx
echo 'dpa_metro_cdmx' > .python-version
pip install -f requirements.txt