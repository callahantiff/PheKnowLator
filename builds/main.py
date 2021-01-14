import os

from flask import Flask, request

import build_phase_1

app = Flask(__name__)


@app.route('/')
def build_phase_1():
    build_phase_1
    return 'Phase 1 Completed!'


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
