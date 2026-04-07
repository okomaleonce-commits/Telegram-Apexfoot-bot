import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def home():
    return "Apex Football Bot is running."

@app.route("/ping")
def ping():
    return jsonify({
        "status": "ok",
        "message": "pong"
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
