#!/bin/bash
rm log/build_info.log
nohup sudo pyleus build streaming_topology/pyleus_topology.yaml > log/build_info.log &
if [ $? -ne 0 ]
then
    echo "Building process was successful."
fi