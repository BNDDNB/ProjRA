from flask import Flask, render_template, request, url_for, redirect, send_from_directory
import csv, sqlite3, sys
import pandas as pd
import backend

'''
declaration of global variables
'''

app = Flask(__name__, static_url_path = '/static')
app.debug = True
regions = ['Western Canada', 'Central Canada', 'Atlantic Canada', 'Northern Canada']
sexs = ['Total - Sex', 'Male', 'Female']
ages = ['Total - Age', '15 to 24 years', '25 to 64 years','25 to 54 years', \
            '25 to 34 years', '35 to 44 years', '45 to 54 years', '55 to 64 years', '65 years and over']
identities = ['','Aboriginal','Non-Aboriginal']


@app.route('/')
def index():
    return render_template("index.html", regions = regions, sexs = sexs, \
        ages = ages, identities = identities, cities = cities)

# def show_map():
#   return send_from_directory('html','results/Heatmap.html')

@app.route('/', methods=['GET', 'POST'])

def proc():
    if request.method == "POST":
        if request.form['submit'] == 'Submit':
            region = request.form.get("Region", None)
            identity = request.form.get("Identity", None)
            sex = request.form.get("Sex", None)
            age = request.form.get("Age", None)
            city = request.form.get("City",None)
            resultfig = backend.plotter(con, age, sex, region, city, identity)

            return render_template("index.html", resultfig = resultfig,  regions = regions, sexs = sexs, \
                ages = ages, identities = identities, cities = cities)
        else:
            return render_template("index.html", showHeatmap = 1,  regions = regions, sexs = sexs, \
                    ages = ages, identities = identities, cities = cities)
            #return render_template("results/Heatmap.html")
    return render_template("index.html", regions = regions, sexs = sexs, \
         ages = ages, identities = identities, cities = cities)


if __name__ == '__main__':
    try:
        con = sqlite3.connect(":memory:", check_same_thread = False)
        backend.init()
        backend.data_reader(con, backend.datafile)
        cities = backend.query_cityList(con)
        app.run(debug=True)
    except:
        print ("Unexpected error:" , sys.exc_info()[0])
    finally:
        con.close()


