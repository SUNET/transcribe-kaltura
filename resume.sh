#!/bin/bash

kill -SIGUSR2 $(pgrep --full "python reach-fetcher.py")
