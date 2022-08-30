from mrjob.job import MRJob 
from mrjob.step import MRStep

class MoviePopularity(MRJob):
    
    def mapper_get_movies(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1

    def reducer_count_movies(self, movie_id, count):
        yield None, (sum(count), movie_id)

    def reducer_sort_movies(self, _, values):
        for count, movie_id in sorted(values, reverse=True):
            yield movie_id, count
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
            MRStep(reducer=self.reducer_sort_movies)
        ]

if __name__ == '__main__':
    MoviePopularity.run()