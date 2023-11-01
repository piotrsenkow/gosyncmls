from flask import Flask, jsonify, request
from flask_limiter import Limiter
import logging
import datetime
from logging.handlers import RotatingFileHandler

app = Flask(__name__)

class CustomRotatingFileHandler(RotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_time = None

    def emit(self, record):
        current_time = datetime.datetime.now()
        if self.last_time:
            time_difference = (current_time - self.last_time).total_seconds()
            record.msg += f" | Time since last request: {time_difference:.3f}s"
        self.last_time = current_time
        super().emit(record)

def get_remote_address():
    return request.remote_addr

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["2 per second"]
)

@app.route("/api_endpoint", methods=["GET"])
@limiter.limit("2 per second")
def api_endpoint():
    return jsonify({"message": "Success!"})

if __name__ == "__main__":
    log_format = "[%(asctime)s.%(msecs)03d] %(levelname)s in %(module)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    handler = CustomRotatingFileHandler("server.log", maxBytes=10000, backupCount=3)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(log_format))

    # Attach the handler to both the Flask app logger and the Werkzeug logger
    app.logger.addHandler(handler)
    logging.getLogger('werkzeug').addHandler(handler)
    app.run(debug=True, port=5000)
