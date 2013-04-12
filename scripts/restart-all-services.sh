#!/bin/bash

fab stop_services remove_logs start_router start_pipeliner start_brokers start_web
