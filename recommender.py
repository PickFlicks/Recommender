from json import loads, dumps
import flask
import sys
from os.path import isfile
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from psycopg2 import connect, DatabaseError
from pandas import read_csv, Series, isnull

#================== Database Config ==================#
database = "pickflix"
user = ''
password = ''
movie_table = 'movies'
recommendation_table = 'recommendations'

#================== Web App Config ==================#
app = flask.Flask(__name__)
host = 'localhost'
port = 3000
keys = ['zVKRozMnB1rpHnkurMPzTkgoP9BarOrG']

#================== Global Variables ==================#
movie_data_file = 'data/all_movies'
setup_status_file = '.recommender.setup'

#================== Database Setup ==================#
def execute_command(command, output = False, names = False):
    con = None
    result = None
    try:
        con = connect(database = database, user = user, password = password)
        cur = con.cursor()
        cur.execute(command)
        if output:
            if names:
                result = []
                identifiers = cur.description
                values = cur.fetchall()[0]
                for i in range(len(identifiers)):
                    result.append((identifiers[i][0], values[i]))
                result = values
            else:
                result = cur.fetchall()
        cur.close()
        con.commit()
    except (Exception, DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    return result

def create_db():
        con = None
        try:
            con = connect(database = 'postgres', user = user, password = password)
            con.autocommit = True
            cur = con.cursor()
            cur.execute('CREATE DATABASE {0};'.format(database))
            cur.close()
            con.commit()
        except (Exception, DatabaseError) as error:
            print(error)
        finally:
            if con is not None:
                con.close()

def create_table_recommendations():
    execute_command('CREATE TABLE {0} (id INT PRIMARY KEY, similarities REAL[])'.format(recommendation_table))

def uniquify(l, char):
    result = ''
    for item in l:
        result += char + item + ' '
    return result

def create_soup(x):
    return uniquify(str(x['genres']).split(';'), 'g') + \
    ('' if isnull(x['release_year']) else 'y' + str(x['release_year']) + ' ') + \
    ('' if isnull(x['original_language']) else x['original_language'] + ' ') + \
    ('' if isnull(x['popularity']) else 'p' + str(x['popularity']) + ' ') + \
    ('' if isnull(x['vote_average']) else 'v' + str(x['vote_average']) + ' ') + \
    uniquify(str(x['keywords']).split(';'), 'k') + \
    uniquify(str(x['cast']).split(';'), 'c') + \
    ('' if isnull(x['director']) else 'd' + str(x['director']))

def seed_table_recommendations():
    sys.stdout.write('\rLoading Data')
    data = read_csv(movie_data_file + '.csv', delimiter=',')
    sys.stdout.write('\rFormatting Data')
    data['soup'] = data.apply(create_soup, axis=1)
    sys.stdout.write('\rCalculating Counts')
    count_matrix = CountVectorizer().fit_transform(data['soup'])
    sys.stdout.write('\rCalculating Similarities')
    matrix = cosine_similarity(count_matrix, count_matrix)
    sys.stdout.write('\rCalculating Indices     ')
    indices = Series(data.index, index=data['id'])
    sys.stdout.write('\rSaving Similarities     ')
    n = len(indices)
    command = 'INSERT into {0}'.format(recommendation_table) + " (id, similarities) VALUES ({0}, '{1}')"
    con = None
    try:
        con = connect(database = database, user = user, password = password)
        cur = con.cursor()
        for row in indices.iteritems():
            cur.execute(command.format(row[0], '{' + ', '.join(str(item) for item in matrix[row[1]]) + '}'))
            j = row[1] / n
            sys.stdout.write("\rSeeding Movie Table: [%-20s] %d%%" % ('='*int(20*j), 100*j))
        cur.close()
        con.commit()
    except (Exception, DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    return indices

def create_table_movies():
    execute_command("""
    CREATE TABLE {0} (
        id INT PRIMARY KEY,
        adult BOOLEAN,
        backdrop_path TEXT,
        belongs_to_collection BIGINT,
        budget INT,
        genres INT[],
        homepage TEXT,
        imdb_id TEXT,
        original_language CHAR(2),
        original_title TEXT,
        overview TEXT,
        popularity REAL,
        poster_path TEXT,
        production_companies INT[],
        production_countries CHAR(2)[],
        release_date DATE,
        revenue INT,
        runtime INT,
        spoken_languages CHAR(2)[],
        status TEXT,
        tagline TEXT,
        title TEXT,
        video BOOLEAN,
        vote_average REAL,
        vote_count INT,
        keywords INT[],
        actors INT[],
        crew INT[],
        index INT
    )
    """.format(movie_table))

def seed_table_movies(indices):
    con = None
    try:
        con = connect(database = database, user = user, password = password)
        cur = con.cursor()
        movie_data = loads(open(movie_data_file + '.json').read())
        n = len(movie_data)
        counter = 0
        for id in movie_data:
            movie = movie_data.get(id)
            details = movie.get("details")
            genres = '{'
            production_companies = '{'
            production_countries = '{'
            spoken_languages = '{'
            keywords = '{'
            cast = '{'
            crew = '{'
            for genre in details.get("genres", []):
                genres += str(genre.get("id")) + ', '
            for production_company in details.get("production_companies", []):
                production_companies += str(production_company.get("id")) + ', '
            for production_country in details.get("production_countries", []):
                production_countries += production_country.get("iso_3166_1") + ', '
            for spoken_language in details.get("spoken_languages", []):
                spoken_languages += spoken_language.get("iso_639_1") + ', '
            for keyword in movie.get("keywords").get("keywords", []):
                keywords += str(keyword.get("id")) + ', '
            for actor in movie.get("actors").get("cast", []):
                cast += str(actor.get("id")) + ', '
            for member in movie.get("actors").get("crew", []):
                crew += str(member.get("id")) + ', '
            cur.execute('''INSERT into {0}
             VALUES ({1}, {2}, '{3}', {4}, {5}, '{6}', '{7}', '{8}', '{9}', '{10}', '{11}',
             '{12}', '{13}', '{14}', '{15}', {16}, {17}, {18}, '{19}', '{20}', '{21}',
             '{22}', {23}, {24}, {25}, '{26}', '{27}', '{28}', {29})'''.format(movie_table,
             details.get('id'),
             details.get("adult", "true"),
             '' if details.get("backdrop_path", None) == None else details.get("backdrop_path").replace("'", "''"),
             'null' if details.get("belongs_to_collection", None) == None else details.get("belongs_to_collection").get('id'),
             0 if details.get("budget", None) == None else details.get("budget"),
             genres[:-2] + '}',
             '' if details.get("homepage", None) == None else details.get("homepage").replace("'", "''"),
             details.get("imdb_id", ""),
             details.get("original_language", ""),
             '' if details.get("original_title", None) == None else details.get("original_title").replace("'", "''"),
             '' if details.get("overview", None) == None else details.get("overview").replace("'", "''"),
             -1 if details.get("popularity", None) == None else details.get("popularity"),
             '' if details.get("poster_path", None) == None else details.get("poster_path").replace("'", "''"),
             production_companies[:-2] + '}',
             production_countries[:-2] + '}',
             'null' if details.get("release_date", "") == "" else "'{0}'".format(details.get("release_date")),
             -1 if details.get("revenue", None) == None else details.get("revenue"),
             -1 if details.get("runtime", None) == None else details.get("runtime"),
             spoken_languages[:-2] + '}',
             details.get("status", ""),
             '' if len(details.get("tagline", "")) < 1 else details.get("tagline").replace("'", "''"),
             '' if details.get("title", None) == None else details.get("title").replace("'", "''"),
             details.get("video", "false"),
             -1 if details.get("vote_average", None) == None else details.get("vote_average"),
             -1 if details.get("vote_count", None) == None else details.get("vote_count"),
             keywords[:-2] + '}',
             cast[:-2] + '}',
             crew[:-2] + '}',
             indices[details.get('id')]))
            counter += 1
            j = (counter) / n
            sys.stdout.write("\rSeeding Movie Table: [%-20s] %d%%" % ('='*int(20*j), 100*j))
        cur.close()
        con.commit()
    except (Exception, DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#================== Setup ==================#
# calculates and saves information needed for recommendations
def setup():
    x = 0
    if isfile(setup_status_file):
        x = int(open(setup_status_file).read())
    if x == 0:
        sys.stdout.write('Creating Database')
        create_db()
        sys.stdout.write('Creating Recommendation Table')
        create_table_recommendations()
        indices = seed_table_recommendations()
        sys.stdout.write('\rRecommendation Table Setup Complete             \n')
        sys.stdout.write('\rCreating Movie Table')
        create_table_movies()
        seed_table_movies(indices)
        with open(setup_status_file, "w") as out:
            out.write('1')
        sys.stdout.write('\rMovie Table Setup Complete                      \n')

#================== Recommendation Route ==================#
@app.route('/api/movies/recommendations', methods = ['GET'])
def get_movie_recommendations():
    params = flask.request.args.to_dict()
    ids = flask.request.args.getlist('ids')
    if 'key' not in params or params.get('key') not in keys:
        return 'all requests must contain a valid API key', 403
    if 'ids' not in params or len(ids) == 0:
        return 'request must contain list of movies to base recommendations on', 400
    #if 'genres' not in params or len(params.get('genres')) == 0:
    #    return 'request must contain list of genres to base recommendations on', 400
    page = 0 if 'page' not in params or int(params.get('page')) < 1 else int(params.get('page')) - 1
    n = 100 // len(ids)
    results = []
    for id in ids:
        # Get the pairwsie similarity scores of all movies with that movie
        x = execute_command('SELECT similarities FROM {0} WHERE id = {1}'.format(recommendation_table, id), True)[0][0]
        sim_scores = list(enumerate(x))
        # Sort the movies based on the similarity scores
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        # Get the scores of the n most similar movies
        sim_scores = sim_scores[1:n]
        # Get the movie indices
        movie_indices = [i[0] for i in sim_scores]
        # Return the top 10 most similar movies
        for index in movie_indices:
            y = execute_command('SELECT id FROM {0} WHERE index = {1}'.format(movie_table, index), True)[0][0]
            data = execute_command('SELECT * FROM {0} WHERE id = {1}'.format(movie_table, y), True, True)
            json_dict = { 'id': data[0], 'adult': data[1], 'backdrop_path': data[2],
              'belongs_to_collection': data[3],
              'budget': data[4],
              'genres': data[5],
              'homepage': data[6],
              'imdb_id': data[7],
              'original_language': data[8],
              'original_title': data[9],
              'overview': data[10],
              'popularity': data[11],
              'poster_path': data[12],
              'production_companies': data[13],
              'production_countries': data[14],
              'release_date': data[15],
              'revenue': data[16],
              'runtime': data[17],
              'spoken_languages': data[18],
              'status': data[19],
              'tagline': data[20],
              'title': data[21],
              'video': data[22],
              'vote_average': data[23],
              'vote_count': data[24],
              'keywords': data[25],
              'actors': data[26],
              'crew': data[27]}
            results.append(dumps(json_dict, default=str))
#        results.extend(data['id'].iloc[movie_indices].tolist())
    return dumps(results, default=str)

if __name__ == '__main__':
    setup()
    sys.stdout.write('Running API \n')
    app.run(host = host, port = port, debug=True)
