"""Lab 3. Basic wordcount

"""

from mrjob.job import MRJob


#This line declares the class lab3, which extends the MRJob format.

class lab3(MRJob):


    def mapper(self, _, line):

        fields = line.split(";")

        try:

            if len(fields)==4:

                yield('length', (len(fields[2]), 1))

                yield('hashtags', (fields[2].count('#'), 1))

        except:

            pass


    def combiner(self, key, counts):

        count = 0

        total = 0

        for val in counts:

            count += val[1]

            total += val[0]

        yield(key, (count, total))


    def reducer(self, key, counts):

        count = 0

        total = 0

        for val in counts:

            count += val[1]

            total += val[0]

        yield(key, (count, total))

# this class will define two additional methods: the mapper method goes here


#and the reducer method goes after this line



#this part of the python script tells to actually run the defined MapReduce job. Note that lab3 is the name of the class

if __name__ == '__main__':

    lab3.run()

