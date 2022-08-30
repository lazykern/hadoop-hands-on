from mrjob.job import MRJob 
from mrjob.step import MRStep

class RatingBreakdown(MRJob):
    
    def mapper_get_ratings(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, rating, values):
        yield rating, sum(values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

if __name__ == '__main__':
    RatingBreakdown.run()