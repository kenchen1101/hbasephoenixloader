#!/bin/bash
ps -ef | grep HBasePerfTestClient | awk '{print $2}' | xargs kill -9

