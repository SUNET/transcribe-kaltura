#!/bin/bash
set -e
if [ ! -d venv ]; then
  python3 -m venv venv
fi
. venv/bin/activate
pip install -U -r requirements.txt
export KALTURAPARTNERSECRET=07d5272ed23fd9d3377fd94cea5e6b6
export TRANSCRIBERTOKENSECRET="testykey"
