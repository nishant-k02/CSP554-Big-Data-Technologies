from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[A-Za-z']+")

class WordCount2(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_groups,
                combiner=self.combiner_sum,
                reducer=self.reducer_sum,
            )
        ]

    def mapper_groups(self, _, line):
        # Do NOT lowercase all words; just check for lowercase a–n
        for w in WORD_RE.findall(line):
            if w and ('a' <= w[0] <= 'n'):
                yield ('a_to_n', 1)
            else:
                yield ('other', 1)

    def combiner_sum(self, key, counts):
        yield key, sum(counts)

    def reducer_sum(self, key, counts):
        yield key, sum(counts)

if __name__ == "__main__":
    WordCount2.run()
