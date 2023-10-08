from flask import Flask,json, send_file,request,make_response, send_from_directory
import json, os
from conservation_area_checker import conservation_area_checker

app = Flask(__name__)


@app.route('/conservation_area_checker/', methods=['GET'])
def conservation_area():
    url = request.args.get('url')
    if not url: 
        examples = os.listdir('examples')
        html = ''
        for file in examples:
            html += f'<a href=?url={request.host_url }examples/{file}>{file}</a><br/>'

        return html
    check_results = conservation_area_checker(url)
    response = dict_to_response(check_results, app)
    return response

@app.route('/examples/<path:path>')
def serve_headshots(path):
    return send_from_directory('examples', path)


if __name__ == "__main__":
  app.run(host='0.0.0.0', port=8000, debug=True)


def dict_to_response(dict, app): 
    
    json_data = json.dumps(dict, indent = 4)

    response = app.response_class(
        response=json_data,
        status=200,
        mimetype='application/json'
    )
    
    return response