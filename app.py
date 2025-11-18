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
    # Require login to view profile
    token = session.get("token")
    if not token:
        return redirect(url_for('hello_world'))

    user_id = database_utils.get_user_id_from_session(token)
    if user_id is None:
        return redirect(url_for('hello_world'))

    username = database_utils.get_user_by_id(user_id)
    reviews = database_utils.get_user_reviews(user_id)
    favorite_actor = database_utils.get_favorite_actor(user_id)
    favorite_director = database_utils.get_favorite_director(user_id)
    # Enrich reviews with movie data (title, year) for template
    enriched = []
    for r in reviews:
        movie = None
        try:
            movie = database_utils.get_movie_by_id(r.get('movie_id'))
        except Exception:
            movie = None
        r['movie'] = movie
        enriched.append(r)

    followers = database_utils.get_followers(user_id)
    followees = database_utils.get_followees(user_id)

    return render_template('myprofile.html', username=username, reviews=enriched, favorite_actor=favorite_actor, favorite_director=favorite_director, followers=followers, followees=followees)

@app.route('/movie/<int:movie_id>')
def movie_view(movie_id):
    movie_data = database_utils.get_movie_by_id(movie_id)
    director_data = database_utils.get_movie_directors_full(movie_id)
    actor_data = database_utils.get_movie_actors_full(movie_id)
    production_company_data = database_utils.get_movie_production_company(movie_id)
    genre_data = database_utils.get_movie_genres(movie_id)
    if movie_data:
        return render_template('movie.html', movie=movie_data,
                       directors=director_data,
                       actors=actor_data,
                       companies=production_company_data,
                       genres=genre_data)
    else:
        return "Movie not found", 404


@app.route('/movie/<int:movie_id>/write_review', methods=['GET', 'POST'])
def write_review(movie_id):
    # Require login
    token = session.get("token")
    if not token:
        return redirect(url_for('hello_world'))

    user_id = database_utils.get_user_id_from_session(token)
    if user_id is None:
        return redirect(url_for('hello_world'))

    if request.method == 'GET':
        movie_data = database_utils.get_movie_by_id(movie_id)
        if not movie_data:
            return "Movie not found", 404
        return render_template('write_review.html', movie=movie_data)

    # POST - handle submitted review
    rating_raw = request.form.get('rating')
    review_text = request.form.get('review')

    try:
        rating = float(rating_raw) if rating_raw is not None and rating_raw != '' else None
    except ValueError:
        rating = None

    # Validate rating: must be a number between 0 and 10 (inclusive)
    if rating is None or rating < 0.0 or rating > 10.0:
        movie_data = database_utils.get_movie_by_id(movie_id)
        error_msg = "Rating must be a number between 0 and 10"
        return render_template('write_review.html', movie=movie_data, error=error_msg, rating=rating_raw, review_text=review_text), 400

    # Call database util to insert review
    database_utils.insert_review(user_id, movie_id, rating, review_text)

    return redirect(url_for('movie_view', movie_id=movie_id))


@app.route('/search', methods=['POST'])
def search_movie():
    # Accepts form with 'title' and optional 'year'
    title = request.form.get('title')
    year_raw = request.form.get('year')
    year = None
    if year_raw:
        try:
            year = int(year_raw)
        except ValueError:
            year = None

    if not title or title.strip() == "":
        return render_template('search_results.html', error='Please provide a movie title.', movies=None)

    movie = database_utils.get_movie(title.strip(), year)
    if movie:
        return redirect(url_for('movie_view', movie_id=movie['id']))
    else:
        return render_template('search_results.html', error=None, movies=None, query=title)


@app.route('/user/<username>')
def user_profile(username):
    uid = database_utils.get_user_id(username)
    if not uid:
        return "User not found", 404

    reviews = database_utils.get_user_reviews(uid)
    enriched = []
    for r in reviews:
        movie = None
        try:
            movie = database_utils.get_movie_by_id(r.get('movie_id'))
        except Exception:
            movie = None
        r['movie'] = movie
        enriched.append(r)

    favorite_actor = database_utils.get_favorite_actor(uid)
    favorite_director = database_utils.get_favorite_director(uid)

    current_user_id = None
    token = session.get('token')
    if token:
        current_user_id = database_utils.get_user_id_from_session(token)

    is_owner = (current_user_id == uid)

    is_following = False
    if current_user_id:
        # get list of usernames the current user follows
        cf = database_utils.get_followees(current_user_id)
        if cf and username in cf:
            is_following = True

    return render_template('user_profile.html', username=username, reviews=enriched, favorite_actor=favorite_actor, favorite_director=favorite_director, is_owner=is_owner, is_following=is_following)


@app.route('/actor/<int:actor_id>')
def actor_view(actor_id):
    actor = database_utils.get_actor_by_id(actor_id)
    if not actor:
        return "Actor not found", 404

    movies = database_utils.get_movies_by_actor(actor_id)
    return render_template('actor.html', actor=actor, movies=movies)


@app.route('/director/<int:director_id>')
def director_view(director_id):
    director = database_utils.get_director_by_id(director_id)
    if not director:
        return "Director not found", 404

    movies = database_utils.get_movies_by_director(director_id)
    return render_template('director.html', director=director, movies=movies)


@app.route('/set_favorite_actor', methods=['POST'])
def set_favorite_actor():
    token = session.get('token')
    if not token:
        return redirect(url_for('hello_world'))
    user_id = database_utils.get_user_id_from_session(token)
    if user_id is None:
        return redirect(url_for('hello_world'))

    actor_name = request.form.get('actor')
    if actor_name and actor_name.strip() != '':
        database_utils.set_favorite_actor_by_name(user_id, actor_name.strip())

    return redirect(url_for('myprofile'))


@app.route('/set_favorite_director', methods=['POST'])
def set_favorite_director():
    token = session.get('token')
    if not token:
        return redirect(url_for('hello_world'))
    user_id = database_utils.get_user_id_from_session(token)
    if user_id is None:
        return redirect(url_for('hello_world'))

    director_name = request.form.get('director')
    if director_name and director_name.strip() != '':
        database_utils.set_favorite_director_by_name(user_id, director_name.strip())

    return redirect(url_for('myprofile'))


@app.route('/user_search', methods=['POST'])
def user_search():
    query = request.form.get('username')
    if not query or query.strip() == '':
        return redirect(url_for('myprofile'))

    users = database_utils.search_users(query.strip())
    current_user = None
    token = session.get('token')
    if token:
        current_user = database_utils.get_user_by_id(database_utils.get_user_id_from_session(token))

    return render_template('user_search_results.html', users=users, query=query, current_user=current_user)


@app.route('/follow_user', methods=['POST'])
def follow_user():
    token = session.get('token')
    if not token:
        return redirect(url_for('hello_world'))
    follower_id = database_utils.get_user_id_from_session(token)
    if follower_id is None:
        return redirect(url_for('hello_world'))

    followee_username = request.form.get('followee')
    if not followee_username:
        return redirect(url_for('myprofile'))
    followee_id = database_utils.get_user_id(followee_username)
    if followee_id is None:
        return "User not found", 404

    if followee_id == follower_id:
        return redirect(url_for('user_profile', username=followee_username))

    try:
        database_utils.insert_follow(follower_id, followee_id)
    except Exception:
        # ignore duplicates or DB errors for now
        pass

    return redirect(url_for('user_profile', username=followee_username))
    


if __name__ == '__main__':

    # run() method of Flask class runs the application 
    # on the local development server.
    app.secret_key = database_utils.API_KEY # No one should know this except us
    app.config['SESSION_TYPE'] = 'memcache'
    app.run()