from flask import Flask, jsonify, request
import json

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route("/test", methods=["POST"])
def home():
    return f"<h1>This is a test API.</p>"


app.run()
