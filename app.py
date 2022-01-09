from flask import Flask

from coruscant.api import (
    measurement_add as measurement_add_api,
    measurements_list as measurements_list_api
)

app = Flask(__name__)


@app.route('/api/measurement/add', methods=['POST'])
def measurement_add():
    return measurement_add_api()


# @app.route('/api/measurement/update')
@app.route('/api/measurements')
def measurements_list():
    return measurements_list_api()


@app.route('/')
def hello():
    return 'Hello, Planetly!'


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
