from flask import app
from flask import Flask, jsonify, request, render_template_string
import csv
from flask_cors import CORS
import pandas as pd
import plotly.express as px
import tasks_binance   # 


app = Flask(__name__, template_folder="templates")
app.app_context().push()  
CORS(app)

from application.controllers import *


@app.route('/api/instruments_csv')
def get_instruments_csv():
    df = pd.read_csv("binance_instruments.csv", encoding="utf-8")
    json_data = df.to_json(orient="records", force_ascii=False)  # keep Chinese characters

    return json_data


@app.route('/chart_datatype')
def chart_datatype():
    """Return a timeline chart (HTML) for a given data_type showing each instrument as a bar from from_date to to_date."""
    data_type = request.args.get('data_type') or request.args.get('datatype')
    if not data_type:
        return "Missing query parameter: data_type", 400

    df = pd.read_csv('binance_instruments.csv', dtype=str)
    df = df[df['datatype'] == data_type].copy()
    
    if df.empty:
        return f"No instruments found for datatype '{data_type}'", 404

    # Parse dates
    df['from_date'] = pd.to_datetime(df['from_date'], errors='coerce')
    df['to_date'] = pd.to_datetime(df['to_date'], errors='coerce')
    # fill missing to_date with from_date
    df['to_date'] = df['to_date'].fillna(df['from_date'])

    # Filter to include only instruments with start date in 2024 or 2025
    # Keep rows where from_date is valid and its year is 2024 or 2025
    df = df[~df['from_date'].isna()].copy()
    df = df[df['from_date'].dt.year.isin([2024, 2025])]

    # Build timeline chart: instruments on y, dates on x
    fig = px.timeline(df, x_start='from_date', x_end='to_date', y='instrument', color='instrument')
    fig.update_yaxes(autorange='reversed')

    chart_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
    return render_template_string('<a href="/">← Back</a>' + chart_html)


if __name__ == "__main__":
    app.run(debug=True, port = 5000 )