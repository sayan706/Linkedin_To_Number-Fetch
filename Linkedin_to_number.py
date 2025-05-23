from flask import Flask, request, send_file, jsonify
import pandas as pd
import duckdb
import requests
import time
import tempfile
import os
import re

app = Flask(__name__)

# Your Hatch API key
HATCH_API_KEY = "Paste APi Key Here"

def limit_linkedins(df, limit):
    query = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY company ORDER BY linkedin) as rn
            FROM df
        ) WHERE rn <= {limit}
    """
    return duckdb.query(query).to_df()

def get_phone_number(linkedin_url):
    url = "https://api.hatchhq.ai/v1/findPhone"
    headers = {"x-api-key": HATCH_API_KEY}
    json = {"linkedinUrl": linkedin_url}
    try:
        response = requests.post(url, headers=headers, json=json)
        response.raise_for_status()
        data = response.json()
        phone = data.get("phone", "Not Found")

        # If it's a list, get the first element
        if isinstance(phone, list) and phone:
            phone = phone[0]

        # Clean phone number (keep digits and leading +)
        if isinstance(phone, str):
            phone = re.sub(r"[^\d+]", "", phone)

        return phone
    except Exception as e:
        print(f"Error for {linkedin_url}: {e}")
        return "Error"

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files or 'limit' not in request.form:
        return jsonify({"error": "CSV file and limit are required"}), 400

    file = request.files['file']
    try:
        limit = int(request.form['limit'])
    except ValueError:
        return jsonify({"error": "Limit must be an integer"}), 400

    df = pd.read_csv(file)
    limited_df = limit_linkedins(df, limit)

    phone_numbers = []
    for _, row in limited_df.iterrows():
        phone = get_phone_number(row["linkedin"])
        phone_numbers.append(phone)
        time.sleep(1)  # Respect API rate limits

    limited_df["phonenumber"] = phone_numbers

    # Save to a temporary file
    temp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    limited_df.to_csv(temp.name, index=False)
    temp.close()

    return send_file(temp.name, as_attachment=True, download_name="output.csv")

if __name__ == '__main__':
    app.run(debug=True)
