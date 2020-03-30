#!/bin/bash
git clone https://github.com/omireyes19/dpa-metro-cdmx.git
cd dpa_metro_cdmx/
pyenv virtualenv 3.7.4 dpa_metro_cdmx
echo 'dpa_metro_cdmx' > .python-version
pip install -f requirements.txt