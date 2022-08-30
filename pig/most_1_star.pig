ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);
   
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;
   
ratingsByMovie = GROUP ratings BY movieID;
averateRatings = FOREACH ratingsByMovie GENERATE group as movieID, AVG(ratings.rating) as avgRating, COUNT(ratings.rating) as countRating;
badMovies = FILTER averateRatings BY avgRating < 2.0;
badMoviesWithData = JOIN badMovies BY movieID, nameLookup BY movieID;
result = ORDER badMoviesWithData BY countRating DESC;
DUMP result;