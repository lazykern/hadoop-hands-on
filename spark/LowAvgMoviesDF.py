from pyspark.sql import SparkSession, Row, functions

ITEM_PATH = "/home/phusitsom/Project/hadoop-hands-on/resource/HadoopMaterials/ml-100k/u.item"
DATA_PATH = "/home/phusitsom/Project/hadoop-hands-on/resource/HadoopMaterials/ml-100k/u.data"


def load_movie_names(item_path):
    movie_names_dict = {}
    with open(item_path, encoding="ISO-8859-1") as file:
        for line in file:
           fields = line.split('|') 
           movie_names_dict[int(fields[0])] = fields[1]
    return movie_names_dict

def parse_input(line):
    fields = line.split()
    return Row(movieID = int(fields[0]), rating=float(fields[2]))

if __name__ == "__main__":

    spark_session = SparkSession.builder.appName("PopularMovies").getOrCreate()

    movie_names = load_movie_names(ITEM_PATH)

    lines = spark_session.sparkContext.textFile(DATA_PATH)

    print(lines)
