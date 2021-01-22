#! /bin/bash

printf '%s %s Some log message \n' $(date +%Y-%m-%d) $(date +%H:%M:%S) >> '{absolute_path}/startup_script.log'

# call build script
cd /PheKnowLator
python build_phase_3.py --app "$1" --rel "$2" --owl "$3"
