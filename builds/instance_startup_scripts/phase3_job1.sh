#! /bin/bash

printf '%s %s PKT: Some log message \n' $(date +%Y-%m-%d) $(date +%H:%M:%S)

# call build script
cd /PheKnowLator
python build_phase_3.py --app subclass --rel no --owl no