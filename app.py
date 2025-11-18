# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
from flask import Flask, render_template, session, request, redirect, url_for
import database_utils



# Flask constructor takes the name of 
# current module (__name__) as argument.
app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')

    if database_utils.verify_user(username, password):
        session["token"] = database_utils.make_session(database_utils.get_user_id(username))
        return redirect(url_for('myprofile'))
    else:
        return render_template('login.html', error=True)

@app.route('/signup')
def signup():
    return render_template('signup.html')

@app.route('/signup_submit', methods=['POST'])
def signup_submit():
    username = request.form.get('username') 
    password = request.form.get('password')
    database_utils.make_user(username, password)
    session["token"] = database_utils.make_session(database_utils.get_user_id(username))
    return redirect(url_for('hello_world'))

@app.route('/myprofile')
def myprofile():
    return render_template('myprofile.html')

@app.route('/movie/<int:movie_id>')
def movie_view(movie_id):
    movie_data = database_utils.get_movie_by_id(movie_id)
    
    if movie_data:
        return render_template('movie.html', movie=movie_data)
    else:
        return "Movie not found", 404

if __name__ == '__main__':

    # run() method of Flask class runs the application 
    # on the local development server.
    app.secret_key = database_utils.API_KEY # No one should know this except us
    app.config['SESSION_TYPE'] = 'memcache'
    app.run()