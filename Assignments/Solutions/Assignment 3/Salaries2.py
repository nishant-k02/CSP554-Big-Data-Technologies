from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import io

def salary_band(s):
    if s >= 100000.0:
        return "High"
    elif s >= 50000.0:
        return "Medium"
    else:
        return "Low"

def parse_salary(raw):
    if raw is None:
        return None
    txt = str(raw).strip().replace("$", "").replace(",", "")
    if txt == "" or txt.lower() == "annualsalary":
        return None
    try:
        return float(txt)
    except ValueError:
        return None

class Salaries2(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)]

    def mapper(self, _, line):
        reader = csv.reader(io.StringIO(line), delimiter='\t')
        row = next(reader, None)
        if not row:
            return

        if not hasattr(self, 'salary_index'):
            self.salary_index = None
            headers = [c.strip().lower() for c in row]
            if "annualsalary" in headers:
                self.salary_index = headers.index("annualsalary")
                return
            else:
                self.salary_index = -1

        try:
            raw_salary = row[self.salary_index]
        except IndexError:
            return

        sal = parse_salary(raw_salary)
        if sal is None or sal < 0:
            return

        yield salary_band(sal), 1

    def combiner(self, band, counts):
        yield band, sum(counts)

    def reducer(self, band, counts):
        yield band, sum(counts)

if __name__ == "__main__":
    Salaries2.run()
