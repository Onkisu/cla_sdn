# ai_service/app.py
from flask import Flask, request, jsonify
from translator_auto import translate_and_apply

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"status":"ok"})

@app.post("/intent")
def intent():
    data = request.get_json(force=True)
    prompt = data.get("prompt","")
    if not prompt:
        return jsonify({"error":"prompt required"}),400
    result = translate_and_apply(prompt)
    return jsonify(result)

if __name__=="__main__":
    app.run(host="0.0.0.0", port=7654)
