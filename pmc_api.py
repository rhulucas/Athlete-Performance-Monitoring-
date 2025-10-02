from datetime import datetime
from pyspark_pmc_functions import duration_avg, peaks_per_session

from flask import Flask, jsonify, request, Response

# FLASK
app = Flask(__name__)


@app.route('/rollingavg/')
def rolling_avg():
    player = request.args.get('player')
    date = request.args.get('date')
    session = request.args.get('session')
    duration = request.args.get('duration')
    column = request.args.get('column')
    if any(v == '' for v in [player, date, session, duration]):
        return jsonify({'status': 'error', 'message': 'Missing parameter.'})

    try:
        qdate = datetime.strptime(date,'%Y-%m-%d').date()
        result = duration_avg(player, session, date, int(duration), column)
        return Response(result.to_json(orient='records'), mimetype='application/json')
    except Exception as err:
        return jsonify({'status': 'error', 'message': str(err)})


@app.route('/peaks/')
def peaks():
    player = request.args.get('player')
    date = request.args.get('date')
    session = request.args.get('session')
    column = request.args.get('column')
    if any(v == '' for v in [player, date, session, column]):
        return jsonify({'status': 'error', 'message': 'Missing parameter.'})
    try:
        qdate = datetime.strptime(date,'%Y-%m-%d').date()
        _, peaks = peaks_per_session(player, session, qdate, column)
        return jsonify({'peaks': peaks.tolist()})
    except Exception as err:
        return jsonify({'status': 'error', 'message': str(err)})