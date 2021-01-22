#! /bin/bash

printf '%s %s Some log message \n' $(date +%Y-%m-%d) $(date +%H:%M:%S) >> '{absolute_path}/startup_script.log'

# make sure authentication is set
gcloud --quiet auth configure-docker

# pull docker container
pull docker "$0"

# call build script
#cd /PheKnowLator
#python build_phase_3.py --app "$1" --rel "$2" --owl "$3"

docker run "$0" sh -c /PheKnowLator/build_phase_3.py --app "$1" --rel "$2" --owl "$3"
