#!/bin/bash

kill -SIGUSR1 $(pgrep --full "python reach-fetcher.py")
