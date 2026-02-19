from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[A-Za-z']+")

class WordCount3(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_len, combiner=self.combiner_sum, reducer=self.reducer_sum)]

    def mapper_len(self, _, line):
        # normalize to lowercase per instructions
        for w in WORD_RE.findall(line.lower()):
            yield len(w), 1

    def combiner_sum(self, key, counts):
        yield key, sum(counts)

    def reducer_sum(self, key, counts):
        yield key, sum(counts)

if __name__ == "__main__":
    WordCount3.run()
