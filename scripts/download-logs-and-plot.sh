#!/bin/bash

fab download_broker_logs
./parse_logs.py
./plot_time_histogram.py
