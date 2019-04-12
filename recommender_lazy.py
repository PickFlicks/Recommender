from json import loads, dumps
import flask
import sys
from os.path import isfile
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from psycopg2 import connect, DatabaseError
from pandas import read_csv, Series, isnull
from scipy.sparse import save_npz, load_npz

#================== Database Config ==================#
database = "pickflix"
user = ''
password = ''
movie_table = 'movies'

#================== Web App Config ==================#
app = flask.Flask(__name__)
host = 'localhost'
port = 3000
keys = ['zVKRozMnB1rpHnkurMPzTkgoP9BarOrG']

#================== Global Variables ==================#
NUM_RESULTS = 100
movie_data_file = 'data/all_movies'
setup_status_file = '.recommender.setup'
dataframe_file = 'data/dataframe.npz'
translations_file = 'data/id<->index.csv'
count_matrix = None
translations = None
calculated_similarities = {}

#================== Database Setup ==================#
def execute_command(command, output = False):
    con = None
    result = None
    try:
        con = connect(database = database, user = user, password = password)
        cur = con.cursor()
        cur.execute(command)
        if output:
            result = []
            identifiers = cur.description
            values = cur.fetchall()[0]
            for i in range(len(identifiers)):
                result.append((identifiers[i][0], values[i]))
            result = values
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

#================== Movie Table Setup ==================#
def create_table_movies():
    execute_command("""
    CREATE TABLE {0} (
        id INT PRIMARY KEY,
        adult BOOLEAN,
        backdrop_path TEXT,
        belongs_to_collection INT,
        budget BIGINT,
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
        revenue BIGINT,
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
        crew INT[]
    )
    """.format(movie_table))

def base_round(num, base=10):
    return base * round(num / base)

def uniquify(l, char):
    result = ''
    for item in l:
        result += char + item + ' '
    return result

def create_soup(x):
    return uniquify(str(x['genres']).split(';'), 'g') + \
    ('' if isnull(x['release_year']) else 'y' + str(x['release_year']//10*10) + ' ') + \
    ('' if isnull(x['original_language']) else x['original_language'] + ' ') + \
    ('' if isnull(x['popularity']) else 'p' + str(base_round(x['popularity'], 2.5)) + ' ') + \
    ('' if isnull(x['vote_average']) else 'v' + str(round(x['vote_average'])) + ' ') + \
    uniquify(str(x['keywords']).split(';'), 'k') + \
    uniquify(str(x['cast']).split(';'), 'c') + \
    ('' if isnull(x['director']) else 'd' + str(x['director']))

def seed_table_movies():
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
            genres = ''
            production_companies = ''
            production_countries = ''
            spoken_languages = ''
            keywords = ''
            cast = ''
            crew = ''
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
             '{22}', {23}, {24}, {25}, '{26}', '{27}', '{28}')
             '''.format(movie_table,
             details.get('id'),
             details.get("adult", "true"),
             '' if details.get("backdrop_path", None) == None else details.get("backdrop_path").replace("'", "''"),
             'null' if details.get("belongs_to_collection", None) == None else details.get("belongs_to_collection").get('id'),
             0 if details.get("budget", None) == None else details.get("budget"),
             '{' + genres[:-2] + '}',
             '' if details.get("homepage", None) == None else details.get("homepage").replace("'", "''"),
             details.get("imdb_id", ""),
             details.get("original_language", ""),
             '' if details.get("original_title", None) == None else details.get("original_title").replace("'", "''"),
             '' if details.get("overview", None) == None else details.get("overview").replace("'", "''"),
             -1 if details.get("popularity", None) == None else details.get("popularity"),
             '' if details.get("poster_path", None) == None else details.get("poster_path").replace("'", "''"),
             '{' + production_companies[:-2] + '}',
             '{' + production_countries[:-2] + '}',
             'null' if details.get("release_date", "") == "" else "'{0}'".format(details.get("release_date")),
             -1 if details.get("revenue", None) == None else details.get("revenue"),
             -1 if details.get("runtime", None) == None else details.get("runtime"),
             '{' + spoken_languages[:-2] + '}',
             details.get("status", ""),
             '' if len(details.get("tagline", "")) < 1 else details.get("tagline").replace("'", "''"),
             '' if details.get("title", None) == None else details.get("title").replace("'", "''"),
             details.get("video", "false"),
             -1 if details.get("vote_average", None) == None else details.get("vote_average"),
             -1 if details.get("vote_count", None) == None else details.get("vote_count"),
             '{' + keywords[:-2] + '}',
             '{' + cast[:-2] + '}',
             '{' + crew[:-2] + '}'
             ))
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

#================== Check Declared Numeric types won't overflow ==================#
def get_or_temp(l):
    if l == None or len(l) == 0:
        return [{'id':0}]
    return l

def check_max_int(movie, details):
    MAX_INT = 2147483647
    MAX_BIGINT = 9223372036854775807
    if movie.get('id', 0) > MAX_INT:
        print('id over ', movie.get('id'))
    elif details.get("belongs_to_collection", None) != None and details.get("belongs_to_collection").get('id', 0) > MAX_INT:
        print('collection id over ', details.get("belongs_to_collection").get('id'))
    elif details.get('budget', None) != None and details.get('budget') > MAX_BIGINT:
        print('budget over ', details.get('budget'), movie.get('id'))
    elif max(genre.get('id', 0) for genre in get_or_temp(details.get('genres'))) > MAX_INT:
        print('genre id over ', details.get('genres'))
    elif max(company.get('id', 0) for company in get_or_temp(details.get('production_companies'))) > MAX_INT:
        print('company id over ', details.get('production_companies'))
    elif details.get('revenue' , None) != None and details.get('revenue') > MAX_BIGINT:
        print('revenue over ', details.get('revenue'))
    elif details.get('runtime', None) != None and details.get('runtime') > MAX_INT:
        print('runtime over ', details.get('runtime'))
    elif details.get('vote_count', None) != None and details.get('vote_count') > MAX_INT:
        print('vote_count over: ', detail.get('vote_count'))
    elif max(keyword.get('id', 0) for keyword in get_or_temp(movie.get("keywords").get("keywords"))) > MAX_INT:
        print('keyword id over ', movie.get("keywords").get("keywords"))
    elif max(actor.get('id', 0) for actor in get_or_temp(movie.get("actors").get("cast"))) > MAX_INT:
        print('actor id over ', movie.get("actors").get("cast"))
    elif max(crew.get('id', 0) for crew in get_or_temp(movie.get("actors").get("crew"))) > MAX_INT:
        print('crew id over ', movie.get("actors").get("crew"))
    else:
        return
    print('EXITING')
    raise SystemExit(0)

def check_seed_table_movies():
    movie_data = loads(open(movie_data_file + '.json').read())
    for id in movie_data:
        movie = movie_data.get(id)
        check_max_int(movie,  movie.get("details"))
    print('done')
    raise SystemExit(0)


#================== Setup ==================#
# calculates and saves information needed for recommendations
def setup():
    global count_matrix
    global translations
    x = 0
    if isfile(setup_status_file):
        x = int(open(setup_status_file).read())
    if x < 1:
        sys.stdout.write('Creating Database')
        create_db()
        sys.stdout.write('\rCreating Movie Table')
        create_table_movies()
        seed_table_movies()
        with open(setup_status_file, "w") as out:
            out.write('1')
    if x < 2:
        sys.stdout.write('\rLoading Data')
        data = read_csv(movie_data_file + '.csv', delimiter=',')
        sys.stdout.write('\rFormatting Data')
        data['soup'] = data.apply(create_soup, axis=1)
        sys.stdout.write('\rCalculating Counts')
        count_matrix = CountVectorizer().fit_transform(data['soup'])
        save_npz(dataframe_file, count_matrix)
        sys.stdout.write('\rCalculating Indices     ')
        id_to_index = Series(data.index, index=data['id'])
        id_to_index.to_csv(translations_file, encoding='utf-8', header=True)
        sys.stdout.write('\r                        ')
        with open(setup_status_file, "w") as out:
            out.write('2')
    count_matrix = load_npz(dataframe_file)
    translations = read_csv(translations_file)
    sys.stdout.write('\rSetup Complete\n')

#================== Recommendation Route ==================#
@app.route('/api/movies/recommendations', methods = ['GET'])
def get_movie_recommendations():
    params = flask.request.args.to_dict()
    ids = [int(x) for x in flask.request.args.getlist('ids')]
    if 'key' not in params or params.get('key') not in keys:
        return 'all requests must contain a valid API key', 403
    if 'ids' not in params or len(ids) == 0:
        return 'request must contain list of movies to base recommendations on', 400
    page = 0 if 'page' not in params or int(params.get('page')) < 1 else int(params.get('page')) - 1

    best = {}
    for id in ids:
        # get index in count_matrix of given movie
        index = translations.loc[translations['id'] == id].iloc[0][1]
        sim_scores = None
        if index in calculated_similarities:
            sim_scores = calculated_similarities.get(index)
        else:
            # calculate all movie similarity values and return as list
            sim_scores = list(enumerate(cosine_similarity(count_matrix[index], count_matrix)[0]))
            # sort list from most similar to least similar
            sim_scores = sorted(sim_scores, key=lambda results: results[1], reverse=True)
            calculated_similarities[index] = sim_scores
        # only get the NUM_RESULTS number of movies, based on page number
        for score in sim_scores[(page*NUM_RESULTS):(page+1)*NUM_RESULTS]:
            # save to dictionary or add values if alread in dictionary
            best[score[0]] = best.get(score[0], 0) + score[1]

    results = []
    # sort all top movies by most to least similar so add to results in order
    for movie in sorted(best.items(), key=lambda kv: kv[1], reverse =  True)[(page*NUM_RESULTS):(page+1)*NUM_RESULTS]:
        # get movie info from psql table
        data = execute_command('SELECT * FROM {0} WHERE id = {1}'.format(movie_table, translations.loc[movie[0]][0]), True)
        # only return movies that haven't come out yet, weren't given by query and have runtime >= 40 min
        if data[0] not in ids and data[19] == 'Released' and data[17] >= 40:
            # add json response with attribute names and values to results list
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
            results.append(loads(dumps(json_dict, default=str)))
    return dumps(results), 200

if __name__ == '__main__':
    setup()
    sys.stdout.write('Running API \n')
    app.run(host = host, port = port, debug=True)
