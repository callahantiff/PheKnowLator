import os
import logging

from flask import Flask, request

from build_phase_1 import *

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)


@app.route('/')
def build_phase_1():
    app.logger.info('Processing default request')
    run_phase_1()
    return 'Phase 1 Completed!'


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
