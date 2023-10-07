from flask import Flask,json, send_file,request,make_response, send_from_directory


app = Flask(__name__)


@app.route('/conservation_area_checker/', methods=['GET'])
def conservation_area_checker():
    
    return 'Hello, World!'


if __name__ == "__main__":
  app.run(host='0.0.0.0', port=8000, debug=True)


def df_to_response(df, app): 
    json_data = df.to_json(orient='records')
    response = app.response_class(
        response=json_data,
        status=200,
        mimetype='application/json'
    )
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response