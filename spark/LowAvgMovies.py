from pyspark import SparkContext, SparkConf

def loadMovieNames(path):
    movieNames = {}
    with open(path, encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            # movie id : movie name
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    # (movie id, (rating, 1.0))
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().setAppName("LowAvgMovies")
    sc = SparkContext(conf=conf)

    movieNames = loadMovieNames("/home/maria_dev/ml-100k/u.item")

    file = sc.textFile("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/ml-100k/u.data")

    movieRatings = file.map(parseInput)

    ratingTotalCount = movieRatings.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    avgRatings = ratingTotalCount.mapValues(lambda x: x[0] / x[1])

    sortedMovieRating = avgRatings.sortBy(lambda x: x[1])

    results = sortedMovieRating.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])
