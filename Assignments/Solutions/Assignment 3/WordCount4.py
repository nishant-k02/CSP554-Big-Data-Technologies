from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Words are letters and apostrophes; adjust if you need hyphens etc.
WORD_RE = re.compile(r"[A-Za-z']+")

class WordCount4(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_bigrams,
                       combiner=self.combiner_sum,
                       reducer=self.reducer_sum)]

    def mapper_bigrams(self, _, line):
        # Lowercase so "Hello there" == "hello there"
        tokens = WORD_RE.findall(line.lower())
        # Emit sliding window bigrams per record
        for i in range(len(tokens) - 1):
            bigram = f"{tokens[i]} {tokens[i+1]}"
            yield bigram, 1

    def combiner_sum(self, bigram, counts):
        yield bigram, sum(counts)

    def reducer_sum(self, bigram, counts):
        yield bigram, sum(counts)

if __name__ == "__main__":
    WordCount4.run()
