#!/bin/bash
set -e
source venv/bin/activate
#export KALTURAPARTNERSECRET=07d5272ed23fd9d3377fd94cea5e6b6
export KALTURAPARTNERSECRET=07d5272ed23fd9d3377fd94cea5e6b6c
export TRANSCRIBERTOKENSECRET="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxMjMsInJvbGUiOiJ1c2VyIiwiZXhwIjoxOTAwMDAwMDAwfQ.R88W4K1AjcOzYfXtTkq8cmSZZbPcTzOspLtPohwnRxQ"
python reach_fetcher.py -vv -pid 408 -kurl https://api.kltr.nordu.net/ -murl http://0.0.0.0:8000 -ktid 0_x4h526ri -wid tom
