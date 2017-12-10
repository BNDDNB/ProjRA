from flask import Flask, render_template, request, url_for, redirect

app = Flask(__name__)
app.debug = True
regions = ['Western Canada', 'Central Canada', 'Atlantic Canada', 'Northern Canada']
sexs = ['Total - Sex', 'Male', 'Female']
ages = ['Total - Age', '15 to 24 years', '25 to 64 years','25 to 54 years', \
			'25 to 34 years', '35 to 44 years', '45 to 54 years', '55 to 64 years', '65 years and over']
identities = ['Total - Population by Registered or Treaty Indian status','Registered or Treaty Indian','Not a Registered or Treaty Indian']


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html", regions = regions, sexs = sexs, ages = ages, identities = identities)

@app.route('/proc', methods=['GET', 'POST'])
def login():
    if request.method == "POST":
        car_brand = request.form.get("Region", None)
        car_brand2 = request.form.get("Identity", None)
        car_brand3 = request.form.get("Sex", None)
        car_brand4 = request.form.get("Age", None)
        if car_brand!=None:
        	#car_brand = "" + value for key, value in car_brand
        	return render_template("index.html", car_brand = car_brand,  regions = regions, sexs = sexs, ages = ages, identities = identities)
    return render_template("index.html", regions = regions, sexs = sexs, ages = ages, identities = identities)

if __name__ == '__main__':
    app.run(debug=True)