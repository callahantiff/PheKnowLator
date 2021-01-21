#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
from datetime import datetime

from build_phase_1 import *  # type: ignore
from build_phase_2 import *  # type: ignore


def main():

    start_time = datetime.now()
    print('\n\n' + '*' * 10 + ' STARTING PHEKNOWLATOR KNOWLEDGE GRAPH BUILD ' + '*' * 10)

    # run phase 1 of build
    print('#' * 35 + '\nBUILD PHASE 1: DOWNLOADING BUILD DATA\n' + '#' * 35)
    # run_phase_1()

    # run phase 2 build
    print('#' * 35 + '\nBUILD PHASE 2: DATA PRE-PROCESSING\n' + '#' * 35)
    # run_phase_2()

    # print build statistics
    runtime = round((datetime.now() - start_time).total_seconds() / 60, 3)
    print('\n\n' + '*' * 5 + ' COMPLETED BUILD PHASES 1-2: {} MINUTES '.format(runtime) + '*' * 5)


if __name__ == '__main__':
    main()
