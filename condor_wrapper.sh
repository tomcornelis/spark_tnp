#!/bin/bash
echo "Setting up environment"
source env.sh
echo "Setup complete"
echo "Will run:"
echo "$@"
eval "$@"
