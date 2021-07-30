from flask import Flask, jsonify, request
from consumer_validation import validate_message
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/62895/Desktop/week4/key.json"
app = Flask(__name__)

schema = {"col_names": "required",
         "col_types": "required",
         "col_values": "required"}

@app.route("/")
def index() -> str:
    return jsonify({"Message": "API Created"})


@app.route('/message', methods=['POST'])
def message() -> str:
    errors = validate_message(request)
    if errors is not None:
        print(errors)
    response = {"error": errors}
    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6900, debug=True)